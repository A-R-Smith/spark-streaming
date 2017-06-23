package mains

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sinks.KafkaRowSink

object Enricher {
  val options = Map("es.nodes" -> "10.6.0.10",
    "es.port" -> "9200",
    "es.nodes.wan.only" -> "true")

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("KafkaReader")
      .getOrCreate()
    import spark.implicits._

    // Create Kafka Sink
    val writer = new KafkaRowSink("geoEnriched", "10.6.0.6:9092")

    // Create streaming dataframe Sensor Kafka topic
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.6.0.6:9092")
      .option("subscribe", "sensor")
      .option("startingOffsets", "latest")
      .load()
      
    // Create static dataframe from user profile
    val user_df = spark.read.format("org.elasticsearch.spark.sql")
      .options(options)
      .load("users/profile")
      
    // Parse out json from sensor topic
    val sensor_df = kafka.select(
        get_json_object(($"value").cast("string"), "$.value").cast("double").alias("value"),
        get_json_object(($"value").cast("string"), "$.type").alias("type"),
        get_json_object(($"value").cast("string"), "$.deviceID").alias("deviceID"),
        get_json_object(($"value").cast("string"), "$.gatewayID").alias("gatewayID"),
        get_json_object(($"value").cast("string"), "$.event_ts").cast("long").alias("event_ts"))
      .withColumn("ts", from_unixtime($"event_ts".divide(1000)))

    // Join the two dataframes selecting only the geo_location from the user/profile
    val join_df = sensor_df.as("s")
      .join(user_df.drop($"deviceList").as("g"), Seq("gatewayID"), "left_outer")
      .select($"s.*", $"g.geo_location", $"g.latitude",$"g.longitude")
      
    // convert dataframe back to json
    val jsonDf = join_df.select(to_json(struct(join_df.columns.map(col): _*)))

    // output dataframe to kafka sink
    val query = jsonDf.writeStream
      .foreach(writer)
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
  