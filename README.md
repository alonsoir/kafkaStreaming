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

	
	I think that dependencies are not right. Not compiling any of provided examples. Dependencies are outdated. Starting from scratch with a new fresh view.

	Dependencies updated. I need to change a bit the code and i have to create topic before running the code.

	Compiling, i have to push data into topic to test it.

	KafkaAndSparkStreaming is working. DONE!

	KafkaSparkStreamingReceiver is working. DONE!

	KafkaAndFlink. PENDING!

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