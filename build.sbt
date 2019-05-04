name := "Chapter9"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.0"

val flinkVersion = "1.8.0"

val kafka_streams_scala_version = "0.2.1"


val kafkaStreamsAvroSerdeVersion = "5.2.1"

val avro4sCoreVersion = "1.9.0"

resolvers ++= Seq(
  //"Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  //"apache snapshots" at "http://repository.apache.org/snapshots/",
  //"confluent" at "https://packages.confluent.io/maven",
  "Maven central" at "http://repo1.maven.org/maven2/",

  // chapter 3
  //"Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  //"apache snapshots" at "http://repository.apache.org/snapshots/",
  //"confluent" at "https://packages.confluent.io/maven/"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  // chapter 3 dependencies
  "org.apache.kafka" % "kafka-streams" % "2.2.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.2.0",
  "org.apache.kafka" % "kafka-clients" % "2.2.0",
  "org.apache.avro"  % "avro" % "1.8.2",
  "io.confluent" % "kafka-streams-avro-serde" % kafkaStreamsAvroSerdeVersion,
  "io.confluent" % "kafka-avro-serializer" % kafkaStreamsAvroSerdeVersion
)

