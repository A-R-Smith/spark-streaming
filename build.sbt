name := "IoTEvents"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.1.+" % "provided",
	"org.apache.spark" %% "spark-catalyst" % "2.1.+" % "provided",
	"org.apache.spark" %% "spark-sql"  % "2.1.+" % "provided",
	"org.apache.spark" %% "spark-streaming" % "2.1.+" % "provided",
	"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.1.+",
	"org.elasticsearch" %% "elasticsearch-spark-20" % "5.3.+"
	

	//
	//"org.apache.logging.log4j" % "log4j-core" % "2.7",
	
	
	//these are for the elastic search sink
   // "org.elasticsearch" % "elasticsearch" % "5.3.1" % "provided",
  //  "org.elasticsearch.client" % "transport" % "5.3.1" % "provided"
       // "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.6"
    //"org.json4s" %% "json4s-jackson" % "3.5.1"
    )
    
assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyMergeStrategy in assembly := {
 case PathList("UnusedStubClass.class") => MergeStrategy.rename
 case x => MergeStrategy.first
}

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
 case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard 
case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}