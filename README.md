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

	/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/bin/java -agentlib:jdwp=transport=dt_socket,address=localhost:49744,suspend=n,server=y -Xdebug -server -Xmx1536M -Didea.managed=true -Dfile.encoding=UTF-8 -jar "/Users/aironman/Library/Application Support/IdeaIC2019.1/Scala/launcher/sbt-launch.jar" --addPluginSbtFile=/private/var/folders/gn/pzkybyfd2g5bpyh47q0pp5nc0000gn/T/idea1.sbt "; set ideaPort in Global := 49598 ; idea-shell"
	Listening for transport dt_socket at address: 49744
	[info] Loading settings for project global-plugins from idea1.sbt ...
	[info] Loading global plugins from /Users/aironman/.sbt/1.0/plugins
	[info] Loading settings for project chapter9-build from plugins.sbt ...
	[info] Loading project definition from /Users/aironman/IdeaProjects/Chapter9/project
	[info] Loading settings for project chapter9 from build.sbt ...
	[info] Set current project to SparkJobs (in build file:/Users/aironman/IdeaProjects/Chapter9/)
	[info] Defining Global / ideaPort
	[info] The new value will be used by Compile / compile, Test / compile
	[info] Reapplying settings...
	[info] Set current project to SparkJobs (in build file:/Users/aironman/IdeaProjects/Chapter9/)
	[IJ]sbt:SparkJobs> run
	[warn] Multiple main classes detected.  Run 'show discoveredMainClasses' to see the list

	Multiple main classes detected, select one to run:

 		[1] chapter9.KafkaAndSparkStreaming
 		[2] chapter9.KafkaSparkStreamingReceiver

	Enter number: 

	In the root folder, run this command:
	~> kafka-console-producer --broker-list localhost:9092 --topic pharma-topic < pharma.txt

	You will see the output in the terminal where you execute run command.

# STATUS

	KafkaAndSparkStreaming is working. DONE!

	KafkaSparkStreamingReceiver is working. DONE!

	KafkaAndFlink. In process. 
		WordCount is working, KafkaStreamingFlink is barely working, i can push data into kafka topic and flink is streaming data, but
		intellij is not working properly because i cannot autocomplete nothing. 

	Apply optimizations. PENDING!

# TROUBLESHOOTING

	I used jdk11 for testing purposes, but spark-2.4.X doesnt work properly with jdk11 
	at the time of this record. 
	
	You will have to downgraded jdk version to 1.8. 

	I use fish shell, so i have to invoke the script provided in gist to change jdk properly.

	Or be sure to compile the project with jdk 1.8 and scala 2.11.X.

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
	