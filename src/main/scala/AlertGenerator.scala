import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.sql.Timestamp

object AlertGenerator {
  val options = Map("es.nodes" -> "10.6.0.10",
                    "es.port"  -> "9200",
           "es.nodes.wan.only" -> "true")
           
  val kafkaURL = "10.6.0.6:9092"
  
  
  val windowDuration = 300 //sliding windowDuration for water (in seconds)
  val slideDuration = 300 //slideDuration for water (in seconds)
  val waterFrequency = 2 //frequency the water device sends a message (in seconds)
  val waterThreshold = windowDuration / waterFrequency -10 //the count of water messages needed to trigger alert
   
  def main(args: Array[String]) {
      val spark = SparkSession
        .builder
        .appName("AlertGenerator")
        .getOrCreate()
        
      val writer = new KafkaRowSink("alerts",kafkaURL)   
      import spark.implicits._
        
      val kafka = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",kafkaURL)
        .option("subscribe", "sensor")
        .option("startingOffsets", "latest")
        .load().as[(String,String,String,Int,BigInt,Timestamp,Int)]
      val settings = spark.read.format("org.elasticsearch.spark.sql")
                        .options(options)
                        .load("alerts/settings")
      val sensor = kafka.select(get_json_object(($"value").cast("string"),"$.value").cast("double").alias("value"),
                             get_json_object(($"value").cast("string"),"$.type").alias("type"),
                             get_json_object(($"value").cast("string"),"$.deviceID").alias("deviceID"),
                             get_json_object(($"value").cast("string"),"$.gatewayID").alias("gatewayID"),
                             get_json_object(($"value").cast("string"),"$.event_ts").alias("event_ts"))
                    .withColumn("ts",from_unixtime($"event_ts".divide(1000)))
      
     
      val water = sensor.filter($"type"==="water")
                          .groupBy(
                                window($"ts", windowDuration+" seconds", slideDuration+" seconds"),
                                $"gatewayID",
                                $"deviceID")   
                          .agg(
                                sum("value"),
                                min("event_ts"),
                                $"type")
                          .orderBy($"window".asc)
                          //.filter($"sum(value)">waterThreshold)
//      val water_join = water.join(settings

      val join_df = sensor.join(settings, Seq("deviceID","type","gatewayID"),"left_outer")
                      .filter(row => {
                        row.getAs[String]("type") match {
//                          case "water" => ????
                          case "gas" => 
                            row.getAs[Double]("value")>row.getAs[String]("threshold").toDouble
                          case "battery" => 
                            row.getAs[Double]("value")<row.getAs[String]("threshold").toDouble                         
                          case "humidity"|"temp" => {
                            val value = row.getAs[Double]("value")
                            value<row.getAs[String]("min-threshold").toDouble ||
                            value>row.getAs[String]("max-threshold").toDouble
                          }     
                          case _  => false
                        }
                      }) //.join(water, Seq("deviceID","type","gatewayID"),"left_outer")
                 
      val query = join_df.writeStream
//                       .foreach(writer)
                    .format("console")
                    .option("truncate", false)                      
                       .outputMode("append")
                       .start()
      val query2 = water.writeStream
//                       .foreach(writer)
              .format("console")
              .option("truncate", false)                      
                 .outputMode("complete")
                 .start()
      
      query.awaitTermination();
      query2.awaitTermination();
  }
}
  