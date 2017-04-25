import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.spark.sql.ForeachWriter
import scala.util.parsing.json.JSONObject
import org.apache.spark.sql.Row


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
        producer.send(new ProducerRecord(topic, JSONObject(m).toString()))
      }

      def close(errorOrNull: Throwable): Unit = {
        producer.close()
      }
 }