README file chapter 9 My notes

This project is a fork of https://github.com/PacktPublishing/Mastering-Apache-Kafka-2.0

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


	~> kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1  --topic pharma
	Created topic "pharma".
	~> kafka-topics --list --zookeeper localhost:2181
	pharma

# STATUS

	I think that dependencies are not right. Not compiling. Dependencies are outdated.

	Dependencies updated. Of course, i need to change a bit the code and i have to create topic before running the code.

	Compiling but not working.
