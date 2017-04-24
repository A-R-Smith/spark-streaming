import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.sql.Timestamp

object KafkaReader {
  def main(args: Array[String]) {
      val spark = SparkSession
        .builder
        .appName("KafkaReader")
        .getOrCreate()
        
      val writer = new KafkaTupleSink("water","10.6.0.6:9092")   
      import spark.implicits._
        
      val kafka = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "10.6.0.6:9092")
        .option("subscribe", "sensor")
        .option("startingOffsets", "latest")
        .load().as[(String,String,String,Int,BigInt,Timestamp,Int)]
        
      val ds = kafka.select(get_json_object(($"value").cast("string"),"$.value").alias("value"),
                             get_json_object(($"value").cast("string"),"$.type").alias("type"),
                             get_json_object(($"value").cast("string"),"$.deviceID").alias("deviceID"),
                             get_json_object(($"value").cast("string"),"$.gatewayID").alias("gatewayID"),
                             get_json_object(($"value").cast("string"),"$.event_ts").alias("event_ts"))
                    .withColumn("ts",from_unixtime($"event_ts".divide(1000)))
                             
//      val ds = kafka.map {
//            row =>
//              implicit val format = DefaultFormats
//              parse(row._2).extract[DeviceData]
//          } 

      val water = ds.filter($"type"==="water")
                    .filter($"value">0.0)
                    .groupBy(
                          window($"ts", "1 minutes", "30 seconds"),
                          $"deviceID")
                    .count()
                    .orderBy($"window".desc)
                    .as[(String,String,String)]
//      val query1 =   water.writeStream
//                    .format("console")
//                    .option("truncate", false)
//                    .outputMode("complete")
//                    .start()
      val query2 = water.writeStream
                       .foreach(writer)
                       .outputMode("complete")
                       .start()

//      query1.awaitTermination();
      query2.awaitTermination();
  }
}
  
case class  DeviceData(deviceID: String, gatewayID: String, event_ts: Long, `type`: String, value: String)