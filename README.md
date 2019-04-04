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

# STATUS

	
	I think that dependencies are not right. Not compiling any of provided examples. Dependencies are outdated. Starting from scratch with a new fresh view.

	Dependencies updated. I need to change a bit the code and i have to create topic before running the code.

	Compiling, i have to push data into topic to test it.

	KafkaAndSparkStreaming is working.

# LINKS

	https://github.com/PacktPublishing/Mastering-Apache-Kafka-2.0

	https://kafka.apache.org/quickstart

	https://www.stratio.com/blog/optimizing-spark-streaming-applications-apache-kafka/

# TROUBLESHOOTING

	I used jdk11 for testing purposes, but spark-2.4.X doesnt work properly, o i will have to downgraded jdk version to 1.8. 
	I use fish shell, so i have to invoke that script to change jdk properly.
	 
	http://apache-spark-user-list.1001560.n3.nabble.com/java-lang-IllegalArgumentException-Unsupported-class-file-major-version-55-td34546.html

	https://gist.github.com/alonsoir/fb0e6e012a33a802a91801ccd68b30f3
