name := "SparkJobs"

version := "1.0"

scalaVersion := "2.11.6"

val sparkVersion = "2.4.1"

val flinkVersion = "1.7.2"

val kafkaStreamScala = "2.2.0"

resolvers ++= Seq(
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "apache snapshots" at "http://repository.apache.org/snapshots/",
  "confluent.io" at "http://packages.confluent.io/maven/",
  "Maven central" at "http://repo1.maven.org/maven2/"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion

  // ,"org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion
  , "org.apache.kafka" %% "kafka-streams-scala" % kafkaStreamScala excludeAll(
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "com.fasterxml.jackson.datatype")
  )
  // , "io.confluent" % "kafka-streams-avro-serde" % "5.2.1"
)

