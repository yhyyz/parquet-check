package poc.helper

import org.apache.spark.sql.functions.udf

object UDF {

  val S3KeySuffix = udf((x: String) => {
    val arr = x.split("/")
    arr.length match {
      case 0 | 1 => ""
      case _ => arr(arr.length - 1)
    }
  })
  val S3KeyPrefix = udf((x: String) => {
    val pos = x.lastIndexOf("/")
    pos match {
      case -1 => x
      case _ => x.substring(0, pos)
    }
  })
}
