import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.sql.Timestamp

object WaterEventGenerator {
  val kafkaURL = "10.6.0.6:9092"
  
  val elasticURL = "10.6.0.10"
  val elasticPORT = 9300
  val elasticClusterName = "iot-pilot-dev"
  
  
  val options = Map("es.nodes" -> elasticURL,
                    "es.port"  -> "9200",
           "es.nodes.wan.only" -> "true")
           
  val windowDuration = 300 //sliding windowDuration for water (in seconds)
  val slideDuration = 300 //slideDuration for water (in seconds)
  val waterFrequency = 2 //frequency the water device sends a message (in seconds)
  val waterThreshold = windowDuration / waterFrequency -10 //the count of water messages needed to trigger alert
   
  def main(args: Array[String]) {
      val spark = SparkSession
        .builder
        .appName("AlertGenerator")
        .getOrCreate()
        
//      val writer = new ElasticRowSink("events","data",elasticURL,elasticPORT,elasticClusterName)   
      val writer = new KafkaRowSink("events",kafkaURL)
      import spark.implicits._
        
      val kafka = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",kafkaURL)
        .option("subscribe", "sensor")
        .option("startingOffsets", "latest")
        .load()
      val settings = spark.read.format("org.elasticsearch.spark.sql")
                        .options(options)
                        .load("events/settings")
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
                                min("event_ts"))
                          .orderBy($"window".asc)
                          .filter($"sum(value)">waterThreshold)

      val query = water
               .writeStream
                 .foreach(writer)
//              .format("console")
//              .option("truncate", false)                      
                 .outputMode("complete")
                 .start()
      
      query.awaitTermination();
  }
}
  