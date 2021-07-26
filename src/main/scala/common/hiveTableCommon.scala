   package common

   import org.apache.spark.sql.{DataFrame, SparkSession}
   import org.slf4j.LoggerFactory


   object hiveTableCommon {
     private val logger = LoggerFactory.getLogger(getClass.getName)	
		
	 def createHiveTable(spark : SparkSession) : Option[DataFrame] = {
		try {				
		  val sampleReq = Seq((1, "Spark"), (2, "Big Data"), (3, "Scala"))
		  val df1 = spark.createDataFrame(sampleReq).toDF(colNames = "course id", "course name")
		  df1.write.format(source = "csv").save(path = "samplesq")
		  Some(df1)
		} catch {
		  case e: Exception =>
			logger.error("Error Reading demo.employee "+e.printStackTrace())
			None
		}
	  
	  
	  def filterTable(df: DataFrame) {
		df2=df.filter(df("course name") == 'Big Data')
    	Some(df2)
	  }
	  
	  }	
	}
	  
	  
	  