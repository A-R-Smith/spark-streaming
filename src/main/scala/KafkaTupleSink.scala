import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.spark.sql.ForeachWriter


 class  KafkaTupleSink(topic:String, servers:String) extends ForeachWriter[(String, String, String)] {
      val kafkaProperties = new Properties()
      kafkaProperties.put("bootstrap.servers", servers)
      kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      //      kafkaProperties.put("key.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
//      kafkaProperties.put("value.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
      //val results = new scala.collection.mutable.HashMap[String, String]
      var producer: KafkaProducer[String, String] = _

      def open(partitionId: Long,version: Long): Boolean = {
        producer = new KafkaProducer(kafkaProperties)
        true
      }

      def process(value: (String, String, String)): Unit = {
          producer.send(new ProducerRecord(topic, "{"+"\"window\":\""+value._1 
                                                 + "\",\"deviceID\":\"" + value._2 
                                                 + "\",\"count\":" + value._3 +"}"))
          
      }

      def close(errorOrNull: Throwable): Unit = {
        producer.close()
      }
   }