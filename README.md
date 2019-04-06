README file

This project is a fork of the provided code of Spark Streaming Processing with Kafka book. 

# Preliminaries

	brew install zookeeper

	brew install java

	brew install kafka


# ZOOKEEPER up and running
	~/I/Chapter9> zkServer start
	ZooKeeper JMX enabled by default
	Using config: /usr/local/etc/zookeeper/zoo.cfg
	Starting zookeeper ... STARTED
	~/I/Chapter9> zkServer status
	ZooKeeper JMX enabled by default
	Using config: /usr/local/etc/zookeeper/zoo.cfg
	Mode: standalone

# KAFKA SERVER up and running 

	~> kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1  --topic pharma-topic
	Created topic "pharma".
	~> kafka-topics --list --zookeeper localhost:2181
	pharma

# TO RUN AND TEST
	
	Clone the project, import it to Intellij or whatever IDE you use.
	Run command sbt run

	Choose the main class

	aironman@MacBook-Pro-Retina-de-Alonso ~/I/Chapter9> sbt run
	[info] Loading global plugins from /Users/aironman/.sbt/1.0/plugins
	[info] Loading settings for project chapter9-build from plugins.sbt ...
	[info] Loading project definition from /Users/aironman/IdeaProjects/Chapter9/project
	[info] Loading settings for project chapter9 from build.sbt ...
	[info] Set current project to Chapter9 (in build file:/Users/aironman/IdeaProjects/Chapter9/)
	[info] Updating ...
	[info] Done updating.
	[warn] There may be incompatibilities among your library dependencies; run 'evicted' to see detailed eviction warnings.
	[info] Compiling 5 Scala sources to /Users/aironman/IdeaProjects/Chapter9/target/scala-2.11/classes ...
	[warn] there were two deprecation warnings; re-run with -deprecation for details
	[warn] one warning found
	[info] Done compiling.
	WARNING: An illegal reflective access operation has occurred
	WARNING: Illegal reflective access by com.google.protobuf.UnsafeUtil (file:/Users/aironman/.sbt/boot/scala-2.12.7/org.scala-sbt/sbt/1.2.8/protobuf-java-3.3.1.jar) to field java.nio.Buffer.address
	WARNING: Please consider reporting this to the maintainers of com.google.protobuf.UnsafeUtil
	WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
	WARNING: All illegal access operations will be denied in a future release
	[warn] Multiple main classes detected.  Run 'show discoveredMainClasses' to see the list

	Multiple main classes detected, select one to run:

 		[1] chapter9.KafkaAndSparkStreaming
 		[2] chapter9.KafkaSparkStreamingReceiver
 		[3] chapter9.KafkaStreamWithFlink
 		[4] chapter9.WordCountFlink
	[info] Packaging /Users/aironman/IdeaProjects/Chapter9/target/scala-2.11/chapter9_2.11-1.0.jar ...
	[info] Done packaging.

	Enter number: 
 

	In the root folder, run this command:
	~> kafka-console-producer --broker-list localhost:9092 --topic pharma-topic < pharma.txt

	You will see the output in the terminal where you execute run command.

# STATUS

	KafkaAndSparkStreaming is working. DONE!

	KafkaSparkStreamingReceiver is working. DONE!

	KafkaAndFlink. DONE!
		WordCount is working, KafkaStreamingFlink is working. 

	Apply optimizations. PENDING!

# TROUBLESHOOTING

	I used jdk11 for testing purposes, but spark-2.4.X doesnt work properly with jdk11 
	at the time of this record. 
	
	You will have to downgraded jdk version to 1.8. 

	I use fish shell, so i have to invoke the script provided in gist to change jdk properly.

	Or be sure to compile the project with jdk 1.8 and scala 2.11.X.

	A recurrent problem happened when i tried to go to any declaration, imports missing. 
	The way to solve it is to change name variable in build.sbt file. 
	There were a name different from the name project, Chapter9. It MUST be the same that name project. 

# LINKS

	https://github.com/PacktPublishing/Mastering-Apache-Kafka-2.0

	https://kafka.apache.org/quickstart

	https://www.stratio.com/blog/optimizing-spark-streaming-applications-apache-kafka/

	https://github.com/AbsaOSS/ABRiS/

	https://github.com/eBay/Spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/DirectKafkaWordCount.scala

	https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html

	http://apache-spark-user-list.1001560.n3.nabble.com/java-lang-IllegalArgumentException-Unsupported-class-file-major-version-55-td34546.html

	https://gist.github.com/alonsoir/fb0e6e012a33a802a91801ccd68b30f3

	https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html

	https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/ConsumerConfig.html

	https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/api/datastream/package-tree.html

	https://ci.apache.org/projects/flink/flink-docs-release-1.3/api/java/org/apache/flink/streaming/api/windowing/time/class-use/Time.html

	https://www.youtube.com/watch?v=NoFfgJbkl-o

	https://github.com/apache/flink/blob/c1683fbd0aaeb9848b03efaa4c17cc4cc159711c/flink-examples/flink-examples-streaming-kafka-0.10/src/main/scala/org/apache/flink/streaming/scala/examples/kafka/Kafka010Example.scala

	https://stackoverflow.com/questions/40210261/the-benefits-of-flink-kafka-stream-over-spark-kafka-stream-and-kafka-stream-ove

	https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/pom.xml

	https://flink.apache.org/flink-architecture.html

	https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples/windowing/SessionWindowing.scala

	
	