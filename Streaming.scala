import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

class Srteaming {
    def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: StructuredSessionization <FileName> <time_field_name> <time_period>")
      System.exit(1)
    }
    val FileName = args(0)
    val time_field_name = args(1)
    val time_period = args(2)
//create session 
val spark = SparkSession
  .builder
  .appName("StreamingApp")
  .getOrCreate()  
 
 spark.sparkContext.setLogLevel("ERROR")

//get .parquet file
val df  = spark
  .readStream
  .option("sep", ",")     
  .parquet(FileName)  //<-- an error may occur here
//process somehow data
//------------------------------------------------------------
//select data by time_field_name
  df.select(time_field_name).show()
//------------------------------------------------------------
//set window time
val windowTimeSelect = df.groupBy(time_period)

//print result to Kafka
writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")//<-- which port and host
    .option("topic", "updates")
    .start()

import spark.implicits._
  }

}
