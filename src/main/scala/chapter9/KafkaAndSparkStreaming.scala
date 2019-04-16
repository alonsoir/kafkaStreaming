package chapter9

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, TaskContext}

/***
  * This class tries to show the first way to get data streaming between spark and kafka 0.10.X.
  * It is the recommended way to use, i.e. instantiate the static createDirectStream method,
  * because it is parallelizable and therefore gives more performance.
  *
  * In previous versions, 0.8.X, there was another way that today is completely discouraged, so much so that currently can not be used in version 0.10.X.
  */
object KafkaAndSparkStreaming {

  /*main method*/
  def main(args: Array[String]) {
    /*logging at error level*/
    Logger.getRootLogger.setLevel(Level.ERROR)

    /*Since spark streaming is a consumer, we need to pass it topics*/
    val topics = Set("pharma-topic")
    /*pass kafka configuration details*/
    val kafkaParameters = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "consumerGroup",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      // DEFAULT values. session.timeout.ms, 10000 in Kafka +0.10.1 and 30000 in Kafka -0.10.0
      ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> "10000",
      // This can be incremented in order to avoid generating consumers losses,
      // only if it is in the range of the properties group.min.session.timeout.ms and group.max.session.timeout.ms
      // If your Spark batch duration is larger than the default Kafka heartbeat session timeout (30 seconds),
      // increase heartbeat.interval.ms and session.timeout.ms appropriately.
      // For batches larger than 5 minutes, this will require changing group.max.session.timeout.ms
      ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG -> "3000",
      // It must be ⅓ of the time of session.timeout.ms and you should increase it as incrementing session.timeout.ms maintaining the proportion.
      ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG -> "300000",
      // The maximum time between polls before generating the consumer loss.    request.timeout.ms, 305000
      ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG -> "305000",
      // It must be bigger than max.poll.interval.ms. connections.max.idle.ms, 540000
      ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG -> "540000"
      // There are properties that can only be changed by editing the server.properties file.
      // To date, it can not be done programmatically.
      // server.properties file
      // message.max.bytes=1000000
      // num.network.threads=3
      // num.io.threads=8
      // background.threads=10
      // queued.max.requests=500
      // socket.send.buffer.bytes=102400
      // socket.receive.buffer.bytes=102400
      // socket.request.max.bytes=104857600
      // num.partitions=1
      // fetch.purgatory.purge.interval.requests=100
      // producer.purgatory.purge.interval.requests=100
      // Quick explanations of the numbers:
      //
      // message.max.bytes:                          This sets the maximum size of the message that the server can
      //                                             receive.
      //                                             This should be set to prevent any producer from inadvertently
      //                                             sending extra large messages and swamping the consumers.
      //                                             The default size is 1000000.

      // num.network.threads:                        This sets the number of threads running to handle the network's
      //                                             request.
      //                                             If you are going to have too many requests coming in, then you need
      //                                             to change this value. Else, you are good to go.
      //                                             Its default value is 3.
      //                                             Set num.network.threads higher based on number of concurrent
      //                                             producers, consumers, and replication factor.
      //
      //                                             The default value of 3 has been set based on field experience,
      //                                             however, you can take an iterative approach
      //                                             and test different values until you find what is optimal for your
      //                                             case.

      // num.io.threads:                             This sets the number of threads spawned for IO operations.
      //                                             This is should be set to the number of disks present at the least.
      //                                             Its default value is 8.
      //                                             A common broker server has 8 disks. This number can be increased.

      // background.threads:                         This sets the number of threads that will be running and doing
      //                                             various background jobs.
      //                                             These include deleting old log files.
      //                                             Its default value is 10 and you might not need to change it.

      // queued.max.requests:                        This sets the queue size that holds the pending messages while
      //                                             others are being processed by the IO threads.
      //                                             If the queue is full, the network threads will stop accepting any
      //                                             more messages.
      //                                             If you have erratic loads in your application, you need to set
      //                                             queued.max.requests to a value at which it will not throttle.

      // socket.send.buffer.bytes:                   This sets the SO_SNDBUFF buffer size, which is used for socket
      //                                             connections. 1048576

      // socket.receive.buffer.bytes:                This sets the SO_RCVBUFF buffer size, which is used for socket
      //                                             connections. 1048576

      // socket.request.max.bytes:                   This sets the maximum size of the request that the server can
      //                                             receive. This should be smaller than the Java heap size you
      //                                             have set. 104857600

      // num.partitions:                             This sets the number of default partitions of a topic you create
      //                                             without explicitly giving any partition size.
      //                                             Number of partitions may have to be higher than 1 for reliability,
      //                                             but for performance (even not realistic :)), 1 is better.
      //
      //                                             Ideally you want to assign the default number of partitions
      //                                             (num.partitions) to at least n-1 servers.
      //                                             This can break up the write workload and it allows for greater
      //                                             parallelism on the consumer side.
      //                                             Remember that Kafka does total ordering within a partition, not
      //                                             over multiple partitions, so make sure you partition intelligently
      //                                             on the producer side to parcel up units of work that might
      //                                             span multiple messages/events.
      //
      //                                             Consumers benefit from this approach, on producers – careful design
      //                                             is recommended.
      //                                             You need to balance the benefits between producer and consumers
      //                                             based on your business needs.
      //
      //                                             Kafka is designed for small messages. I recommend you to avoid
      //                                             using kafka for larger messages.
      //                                             If that’s not avoidable there are several ways to go about sending
      //                                             larger messages like 1MB.
      //                                             Use compression if the original message is json, xml or text using
      //                                             compression is the best option to reduce the size.
      //                                             Large messages will affect your performance and throughput.
      //                                             Check your topic partitions and replica.fetch.size to make sure
      //                                             it doesn’t go over your physical ram.
      //                                             Another approach is to break the message into smaller chunks and
      //                                             use the same message key to send it same partition.
      //                                             This way you are sending small messages and these can be
      //                                             re-assembled at the consumer side.
      //
      //                                             This complicates your Producer and Consumer code in case of
      //                                             very large messages.
      //                                             Design carefully how Producers and Consumers deal with large
      //                                             size messages.
      //                                             Emphasizing this point is very important!.
      //                                             There are many ways to implement compression or chunking,
      //                                             as well decompression and assembly.
      //                                             Choose after testing your approach.
      //                                             For example, a high compression ratio is most of the time an
      //                                             advantage but it comes with a price paid for
      //                                             compression/decompression time.
      //                                             It is sometimes more efficient to have a lesser compression as long
      //                                             as you can reduce the size of the message under 1MB,
      //                                             but faster compression/decompression.
      //                                             It all comes-down to your SLAs whether they are ms or seconds.

      //                                             These are no silver bullet :), however, you could test these
      //                                             changes with a test topic and 1,000/10,000/100,000 messages
      //                                             per second to see the difference between default values and
      //                                             adjusted values. Vary some of them to see the difference.
      //
      //                                             You may need to configure your Java installation for maximum
      //                                             performance.
      //                                             This includes the settings for heap, socket size, and so on.
      //                                             Follow the scientific method, observe, launch hypotheses, create
      //                                             the experiment with the parameters,
      //                                             collect the data and check if it fits your hypothesis.
      //                                             If it does, do peer review and let
      //                                             the community have their say so that we can all learn.

      // fetch.purgatory.purge.interval.requests:    Specify the interval in number of requests that triggers
      //                                             the purging of the fetch requests.100

      // producer.purgatory.purge.interval.requests: Specify the interval in number of requests that triggers
      //                                             the purging of the producer requests. 100

      // Thank you, @Randy Gelhausen and @Constantin Stanca for the references.
    )

    /*
    Making partitions in Kafka over the topics which are going to be consumed is very important,
    hence this will allow you to parallelize the reception of the events in different Spark executors.
    Creating a relation partition-executor will make every executor receive a chunk of data from the Kafka topic.
    Below we can see how to build a topic with three partitions and the configuration needed to run the Spark job
    with three executors using two cores each and 2GB of RAM.

    ./bin/kafka-topics.sh --create --replication-factor 1 --partitions 3 --zookeeper zookeeper:2181 --topic pharma-topic

    conf.set("spark.cores.max", 6)
    conf.set("spark.executor.cores", 2)
    conf.set("spark.executor.memory", 2GB)

    https://www.stratio.com/blog/optimizing-spark-streaming-applications-apache-kafka/

    https://stackoverflow.com/questions/26562033/how-to-set-apache-spark-executor-memory

    https://www.ibm.com/support/knowledgecenter/en/SSPFMY_1.3.5/com.ibm.scala.doc/config/iwa_cnf_scldc_kfk_prp_exmpl_c.html

    https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster

    */

    /*create spark configurations*/
    val sparkConf = new SparkConf()
                        .setAppName("KafkaAndSparkStreaming")
                        .setMaster("local[*]")
                        // 3 partitions, 3 executors using two cores each with 2GB ram.
                        // One executor per partition. spark.cores.max equal to number partitions multiplied by
                        // spark.executor.cores
                        .set("spark.cores.max","6")
                        .set("spark.executor.cores","2")
                        .set("spark.executor.memory","2g")
                        // Optimize the executor election when Spark compute one task,
                        // this have direct impact into the Scheduling Delay.
                        .set("spark.locality.wait","100")
                        // Activating the BackPressure resulted in the stable performance of a streaming application.
                        // You should activate it in Spark production environments with Kafka
                        .set("spark.streaming.backpressure.enabled","true")
                        // Probably, the most important configuration parameter assigned to the Kafka consumer.
                        // This function is very delicate because it is the one which returns the records to Spark
                        // requested by Kafka by a .seek.
                        // If after two attempts there is a timeout, the Task is FAILED and sent to another
                        // Spark executor causing a delay in the streaming window.
                        // If the timeout is too low and the Kafka brokers need more time to answer there will be
                        // many “TASK FAILED”s,
                        // which causes a delay in the preparation of the assigned tasks into the Executors.
                        // However, if this timeout is too high the Spark executor wastes a lot of time doing nothing
                        // and produces large delays (pollTimeout + task scheduling in the new executor).
                        //The default value of the Spark version 2.x. was 512ms, which could be a good start.
                        // If the Spark jobs cause many “TASK FAILED”s you would need to RAISE that value and
                        // investigate why the Kafka brokers
                        // took so long to send the records to the poll.
                        .set("spark.streaming.kafka.consumer.poll.ms","512")
    /*create spark streaming context*/
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    /*There are multiple ways to get data from kafka topic using KafkaUtils class.
     * One such method to get data using createDirectStream where this method pulls data using direct stream approach*/
    val rawStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParameters))

    /*Get metadata of the records and perform analytics on records*/
    rawStream.foreachRDD { rdd =>
      /*
         Get offset ranges of partitions that will be used to get partition and offset information
         and also this information will be used to commit the offset.
         The offsets indicate where the groupld assigned to the Spark consumer is reading from.
         This is very important because it guarantees the HA during the streaming process and avoids losing data
         if there is an error.
       */
      val rangeOfOffsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      /*get partition information*/
      rdd.foreachPartition { iter =>
        val metadata = rangeOfOffsets(TaskContext.get.partitionId)
        /*print topic, partition, fromoofset and lastoffset of each partition*/
        println(s"topic: ${metadata.topic} " +
                s"partition: ${metadata.partition} " +
                s"fromOffset: ${metadata.fromOffset} " +
                s"untilOffset: ${metadata.untilOffset}")
      } // rdd.foreachPartition
      /*using SQL on top of streams*/
      /*create SQL context*/
      val sqlContext = SparkSession.builder().getOrCreate().sqlContext

      val data = rdd.map(_.value().split(",").to[List]).map(Utils.row)

      /*convert rdd into dataframe by providing header information*/
      val pharmaAnalyticsDF = sqlContext.createDataFrame(data, Utils.schema)
      /*create temporary table*/
      pharmaAnalyticsDF.registerTempTable("pharmaAnalytics")

      /*find total gdp spending per country*/
      val gdpByCountryDF = sqlContext.sql("select country, sum(pc_gdp) as sum_pc_gdp from pharmaAnalytics group by country")
      gdpByCountryDF.show(100,false)

      sqlContext.sql("select country, time, pc_gdp, sum(pc_gdp) as sum_pc_gdp from pharmaAnalytics group by country,time,pc_gdp order by country")
                .show(10,false)
      /*
        Commit the offset after all the processing is completed.
        Using the new API to manage the offsets is not necessary neither to make the implementation
        nor to activate the checkpoint when associated with low performance moments.
        With the following code lines you can keep the offsets in Kafka at any time because the data has been processed correctly
       */
      rawStream.asInstanceOf[CanCommitOffsets].commitAsync(rangeOfOffsets)
    } // rdd.forEach

    ssc.start()
    ssc.awaitTermination()
  } // main
}

