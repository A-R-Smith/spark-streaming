package mains

import java.util.Objects
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sinks.KafkaRowSink

object EventGenerator {
  val kafkaURL = "10.6.0.6:9092,10.6.0.16:9092,10.6.0.18:9092"

  val elasticURL = "10.6.0.10"
  val elasticPORT = 9300
  val elasticClusterName = "iot-pilot-dev"

  val options = Map("es.nodes" -> elasticURL,
    "es.port" -> "9200",
    "es.nodes.wan.only" -> "true")

  val windowDuration = 300 //sliding windowDuration for water (in seconds)
  val slideDuration = 300 //slideDuration for water (in seconds)
  val waterFrequency = 2 //frequency the water device sends a message (in seconds)
  val waterThreshold = windowDuration / waterFrequency - 10 //the count of water messages needed to trigger alert

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("AlertGenerator")
      .getOrCreate()

    //      val writer = new ElasticRowSink("events","data",elasticURL,elasticPORT,elasticClusterName)   
    val writer = new KafkaRowSink("events", kafkaURL)
    import spark.implicits._

    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaURL)
      .option("subscribe", "sensor")
      .option("startingOffsets", "latest")
      .load()

    val settings = spark.read.format("org.elasticsearch.spark.sql")
      .options(options)
      .load("events/settings")
    val sensor = kafka.select(get_json_object(($"value").cast("string"), "$.value").cast("double").alias("value"),
      get_json_object(($"value").cast("string"), "$.type").alias("type"),
      get_json_object(($"value").cast("string"), "$.deviceID").alias("deviceID"),
      get_json_object(($"value").cast("string"), "$.gatewayID").alias("gatewayID"),
      get_json_object(($"value").cast("string"), "$.event_ts").cast("long").alias("event_ts"))
    //  .withColumn("ts",from_unixtime($"event_ts".divide(1000)))

    val join_df = sensor.join(settings, Seq("deviceID", "type", "gatewayID"), "left_outer")
      .filter(row => {
        //System.out.println(row.getAs[String]("type") +"  "+ row.getAs[Double]("value"));
        row.getAs[String]("type") match {
          //                          case "water" => ????
          case "gas" =>

            val thresh = row.getAs[Double]("threshold")
            if (Objects.nonNull(thresh)) {
              row.getAs[Double]("value") > thresh.toDouble
            } else {
              false
            }
          case "battery" =>
            val thresh = row.getAs[Double]("threshold")
            if (Objects.nonNull(thresh)) {
              row.getAs[Double]("value") < thresh.toDouble
            } else {
              false
            }
          case "humidity" | "temp" => {
            val minThresh = row.getAs[Double]("minThreshold")
            val maxThresh = row.getAs[Double]("maxThreshold")
            val value = row.getAs[Double]("value")
            if (Objects.nonNull(minThresh) && Objects.nonNull(maxThresh)) {
              value < minThresh.toDouble || value > maxThresh.toDouble
            } else {
              false
            }
          }
          case _ => false
        }
      })

    //convert data frame to json
    val jsonDf = join_df.select(to_json(struct(join_df.columns.map(col): _*)))

    val query = jsonDf
      .writeStream
      .foreach(writer)
      //                    .format("console")
      //                    .option("truncate", false)                      
      .outputMode("append")
      .start()

    query.awaitTermination();
  }
}
  