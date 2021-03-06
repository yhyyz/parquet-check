package poc.helper

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger}
import poc.ParquetCCTools.{CalcResult, log}
import poc.conf.Config

object ProcessTask {

  /**
   * 开始对每个采样的文件判断，是否是parquet压缩，同时计算压缩后的数据大小， 这里的计算有两点需要注意
   * 1. 将采用文件读取到了driver端的内存里然后串行计算，所以生成的采样文件如果按分区级别计算可能会很大，最好能有metastore信息按表级别计算
   * 因为客户说也就7000张表，所以最多也就是7000次计算
   * 2. 将串行计算的结果保存到了一个List中，也就是内存中，所以如果按分区基本计算就会很大，占用内存就会很大。 所以最好还是有metastore信息.
   * 3. 当前是串行计算，可以优化为并行计算，手动实现读取文件然后转parquet+compression的代码，会麻烦点。当前为了快速出结果
   */
  def processSampleData(x: Row, parmas: Config, ss: SparkSession, taskIndex: String, log: Logger): CalcResult = {

    log.info(s"start task : ${taskIndex}" )
    try {
    val bucket = x.getAs[String]("bucket")
    val pk_sample_key = x.getAs[String]("pk_sample_key")
    val pk_sample_key_size = x.getAs[String]("pk_sample_key_size")
    val pk_total_size = x.getAs[String]("pk_total_size")
    val pk = x.getAs[String]("pk")
    val calcRes = CalcResult(pk, pk_total_size, bucket, pk_sample_key, pk_sample_key_size, "", "")

      val filePath = parmas.fs+"://" + bucket + "/" + pk_sample_key
      val hdfsConf = new Configuration()
      val inputPath = new Path(filePath)
      val inputFileStatus: FileStatus = inputPath.getFileSystem(hdfsConf).getFileStatus(inputPath)
      var codec = ""
      try {
        val footers = ParquetFileReader.readFooters(hdfsConf, inputFileStatus, false)
        // check paruqet
        codec = footers.get(0).getParquetMetadata.getBlocks.get(0).getColumns.get(0).getCodec.toString
      } catch {
        case e: java.io.IOException => {
          log.error(s"task ${taskIndex} process error  ,maybe file is not parquet file:$filePath ")
          return calcRes
        }
      }
      val tmpOutPath = parmas.tmp_data_dir + "/" + taskIndex.toString
      // read parquet file and compress
      ss.read.parquet(filePath).repartition(1).write.mode("overwrite").option("compression", "gzip").parquet(tmpOutPath)
      // check compressed size
      var compressLen = 0L;
      val fs = new Path(tmpOutPath).getFileSystem(hdfsConf)
      val iterator = fs.listFiles(new Path(tmpOutPath), true)
      var flag = true;
      while (iterator.hasNext && flag) {
        val file = iterator.next()
        if (file.getPath.toString.contains("parquet")) {
          flag = false
          compressLen = file.getLen
        }
      }
      //      println(compressLen)
      val res = CalcResult(pk, pk_total_size, bucket, pk_sample_key, pk_sample_key_size, codec, String.valueOf(compressLen))
      // delete tmp file
//      fs.delete(new Path(tmpOutPath), true)
      log.info("finish task : " + taskIndex)
      res
    } catch {
      case e: Exception =>
        log.error(s"process task $taskIndex error: ${e.getMessage}" )
        CalcResult("error","","","","","","")
    }
  }

}
