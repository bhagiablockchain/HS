import common.{postgressCommon, hiveCommon, sparkSession}
import org.slf4j.LoggerFactory


object SparkTransformer {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    try {
      logger.info("Main method started")
      logger.warn("This is a warning")
      
	  
	  val spark = sparkSession.createSparkSession().get
	  

      val data = hiveTableCommon.createHiveTable(spark).get
      data.show()
	  
	  
      val newData = hiveTableCommon.filterTable(data).get
	  newData.show()
	  
      logger.info("Job Done ..")
    }
  }
}
