package poc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory
import poc.conf.Config
import poc.helper.{ProcessTask, SparkHelper, UDF}

import collection.JavaConverters._
import java.util
import java.util.concurrent.{Callable, Executors, Future, TimeUnit}

object ParquetCCTools {
  private val log = LoggerFactory.getLogger("ParquetCCTools")

  case class CalcResult(pk: String, pk_total_size: String, bucket: String, pk_sample_key: String, pk_sample_key_size: String, codec: String, compressLen: String)

  def main(args: Array[String]): Unit = {
    log.info("input params: " + args.mkString(" ,"))
    Logger.getLogger("org").setLevel(Level.WARN)
    val parmas = Config.parseConfig(ParquetCCTools, args)
    val ss = SparkHelper.getSparkSession(parmas.env)
    log.info("start read inventry data.")
    val df = ss.read.option("header", "true").csv(parmas.inventry_s3_csv_path)
    ss.udf.register("S3KeyPrefix", UDF.S3KeyPrefix)
    ss.udf.register("S3KeySuffix", UDF.S3KeySuffix)
    df.createOrReplaceTempView("s3_inventry")
    /**
     * 按照s3上分区粒度统计文件总大小，同时每个分区采样一个parquet文件，用于后续判断该parquet是否被压缩，以及对其进行压缩，获得压缩比
     * 因为现在没有metastore信息(主要表在s3的路径前缀)，只能以表分区级别进行统计，所以要采样的文件会比较多，比如1000表，每个表有5个分区，那么需要采样的文件就会
     * 有 1000 * 5 = 50000 个文件，因此后续的压缩计算会比较慢，如果有metastore信息，只需要每个表采样就可以，这样需要采样的只有1000个文件
     * 因此有了metastore信息后，修改下面的sql和metastore做个join获取表级别采样即可
     */
    val sql =
      s"""
        |with t1 as
        |(select bucket, S3KeyPrefix(key) as prefix , S3KeySuffix(key) as suffix, size,key
        |from s3_inventry ),
        |t2 as
        |(select  key , size ,bucket, CONCAT(bucket,"/",prefix) as pk, ROW_NUMBER() OVER(PARTITION BY  CONCAT(bucket,"/",prefix) order by size desc)  as rn
        |from t1 where t1.suffix!="" and  size >${parmas.sample_file_min} and size <${parmas.sample_file_max}),
        |t3 as
        |(select CONCAT(bucket,"/",prefix) as pk ,sum(size) as pk_total_size from t1 group by CONCAT(bucket,"/",prefix))
        |
        |select t3.pk,cast(cast(t3.pk_total_size as  bigint) as string) as pk_total_size ,t2.bucket,t2.key as pk_sample_key,cast (cast(t2.size as bigint)  as string)as pk_sample_key_size
        | from t3 left join t2 on t3.pk = t2.pk
        | where t2.rn=1
        |
        |""".stripMargin

    val sampleData = ss.sql(sql)
//    sampleData.limit(10).show(false)
    // 将计算后的采样数据存储到一个文件中
    val sampleDataFile = parmas.smaple_data_output_path
    sampleData.repartition(1).write.mode("overwrite").option("header", "true").csv(sampleDataFile)
    log.info("save sample data success, start to process...")
    // 读取采用数据数据文件
    val sampleDataCsv = ss.read.option("header", "true").csv(sampleDataFile + "/*")

    val resList = new util.ArrayList[Future[CalcResult]]()
    val executors = Executors.newFixedThreadPool(parmas.thead_num.toInt)
    sampleDataCsv.collect().foreach(x => {
      //      ProcessTask.processSampleData(x,parmas, ss,taskIndex)
      val task = executors.submit(new Callable[CalcResult] {
        override def call(): CalcResult = {
          val ctname = Thread.currentThread().getName+"-"+System.currentTimeMillis()
          val res = ProcessTask.processSampleData(x, parmas, ss, ctname, log)
          res
        }
      })
      resList.add(task) //添加集合里面
    })

    val finalRes = new util.ArrayList[CalcResult]()
    resList.asScala.foreach(result => {
      finalRes.add(result.get())
    })
    executors.shutdown()
    import ss.implicits._
    log.info("process success, save result.")
    ss.createDataset(finalRes).
      repartition(1).write.mode("overwrite").option("header", "true").csv(parmas.result_dir)
    //    resList.asScala.foreach(result=>{
    //      log.info(result.get())
    //    })
    ss.stop()
    log.info("job done!")
  }
}
