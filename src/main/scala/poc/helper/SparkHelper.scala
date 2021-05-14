package poc.helper

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkHelper {


  def getSparkSession(env: String) = {

    env match {
      case "prod" => {
        val conf = new SparkConf()
          .setAppName("ParquetCCTools")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        SparkSession
          .builder()
          .config(conf)
          .getOrCreate()

      }

      case "dev" => {
        val conf = new SparkConf()
          .setAppName("ParquetCCTools DEV")
          .setMaster("local[3]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        SparkSession
          .builder()
          .config(conf)
          .getOrCreate()
      }
      case _ => {
        println("not match env, exits")
        System.exit(-1)
        null

      }
    }
  }


}
