//import java.util.Properties
//import org.apache.spark.sql.ForeachWriter
//import scala.util.parsing.json.JSONObject
//import org.apache.spark.sql.Row
//import org.elasticsearch.client.Client;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//
//class  ElasticRowSink(esindex:String ,estype:String, server:String, port:Int, clusterName:String) extends ForeachWriter[Row] {
//
//   
//      var client:Client = _
//
//
//      def open(partitionId: Long,version: Long): Boolean = {
//        val settings = Settings.builder()
//          .put("cluster.name", clusterName)
//          .build();   
//        client = new PreBuiltTransportClient(settings)
//                    .addTransportAddress(new InetSocketTransportAddress(
//                            new InetSocketAddress(InetAddress
//                                    .getByName(server), port)));
//        true
//      }
//
//      def process(value: Row): Unit = {
//        val m = value.getValuesMap(value.schema.fieldNames)
//        client.prepareIndex(esindex, estype)
//                  .setSource(JSONObject(m).toString())
//                  .execute().actionGet()
//                
//      }
//
//      def close(errorOrNull: Throwable): Unit = {
//        client.close()
//      }
// }