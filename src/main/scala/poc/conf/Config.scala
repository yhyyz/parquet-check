package poc.conf

case class Config(
                   env: String = "",
                   inventry_s3_csv_path:String = "",
                   smaple_data_output_path:String = "",
                   tmp_data_dir:String ="",
                   fs:String ="s3a",
                   result_dir:String ="",
                   thead_num:String="50",
                   sample_file_min:String = "100000000",
                   sample_file_max:String = "150000000"
                 )


object Config {

  def parseConfig(obj: Object,args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[Config]("spark parquet "+programName) {
      head(programName, "1.0")
      opt[String]('e', "env").required().action((x, config) => config.copy(env = x)).text("env: dev or prod")
      opt[String]('i', "inventry_s3_csv_path").required().action((x, config) => config.copy(inventry_s3_csv_path = x)).text("inventry_s3_csv_path: inventry file ")
      opt[String]('s', "smaple_data_output_path").required().action((x, config) => config.copy(smaple_data_output_path = x)).text("smaple_data_output_path: save sample data dir")
      opt[String]('t', "tmp_data_dir").required().action((x, config) => config.copy(tmp_data_dir = x)).text("tmp_data_dir: tmp data dir ")
      opt[String]('r', "result_dir").required().action((x, config) => config.copy(result_dir = x)).text("result_dir: save result ")
      opt[String]('n', "thead_num").optional().action((x, config) => config.copy(thead_num = x)).text("thead_num: default 10 ")
      opt[String]('m', "sample_file_min").optional().action((x, config) => config.copy(sample_file_min = x)).text("sample_file_min: default 100000000 bytes")
      opt[String]('x', "sample_file_max").optional().action((x, config) => config.copy(sample_file_max = x)).text("sample_file_max: default 150000000 bytes")
      opt[String]('f', "s3").optional().action((x, config) => config.copy(fs = x)).text("file system: default s3a")

    }
    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
        System.exit(-1)
        null
      }
    }

  }

}
