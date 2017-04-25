import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.sql.Timestamp

object Enricher {
  val options = Map("es.nodes" -> "10.6.0.10",
                    "es.port"  -> "9200",
           "es.nodes.wan.only" -> "true")
   
  def main(args: Array[String]) {
      val spark = SparkSession
        .builder
        .appName("KafkaReader")
//        .config("es.nodes","10.6.0.10")
//        .config("es.port","9200")
//        .config("es.nodes.wan.only","true")
        .getOrCreate()
        
      val writer = new KafkaRowSink("water","10.6.0.6:9092")   
      import spark.implicits._
        
      val kafka = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "10.6.0.6:9092")
        .option("subscribe", "sensor")
        .option("startingOffsets", "latest")
        .load().as[(String,String,String,Int,BigInt,Timestamp,Int)]
      val geo_df = spark.read.format("org.elasticsearch.spark.sql")
                        .options(options)
                        .load("geo/location")
      val ds = kafka.select(get_json_object(($"value").cast("string"),"$.value").alias("value"),
                             get_json_object(($"value").cast("string"),"$.type").alias("type"),
                             get_json_object(($"value").cast("string"),"$.deviceID").alias("deviceID"),
                             get_json_object(($"value").cast("string"),"$.gatewayID").alias("gatewayID"),
                             get_json_object(($"value").cast("string"),"$.event_ts").alias("event_ts"))
                    .withColumn("ts",from_unixtime($"event_ts".divide(1000)))
                             
      val join_df = ds.join(geo_df, Seq("gatewayID"),"left_outer")
                     // .select("deviceID", "location", "name")
                   //   .as[(String,String,String)]
//      val query1 =   join_df.writeStream
//                    .format("console")
//                    .option("truncate", false)
//                    .outputMode("append")
//                    .start()
      val query2 = join_df.writeStream
                       .foreach(writer)
                       .outputMode("append")
                       .start()

//      query1.awaitTermination();
      query2.awaitTermination();
  }
}
  