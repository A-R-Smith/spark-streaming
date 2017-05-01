import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL.WithDouble._




 class  KafkaRowSink(topic:String, servers:String) extends ForeachWriter[Row] {
      val kafkaProperties = new Properties()
      kafkaProperties.put("bootstrap.servers", servers)
      kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      var producer: KafkaProducer[String, String] = _

      def open(partitionId: Long,version: Long): Boolean = {
        producer = new KafkaProducer(kafkaProperties)
        true
      }

      def process(value: Row): Unit = {
        val m = value.getValuesMap(value.schema.fieldNames)
        var json = "{";
        m.foreach(p=> {
          json = json + "\"" + p._1 + "\":\"" + p._2 + "\","
        })
        json = json.dropRight(1) + "}" // dropRight removes last comma
        producer.send(new ProducerRecord(topic, json))
      }

      def close(errorOrNull: Throwable): Unit = {
        producer.close()
      }
      
      
 }