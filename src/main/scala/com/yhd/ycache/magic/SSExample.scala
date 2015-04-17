
package com.yhd.ycache.magic

import java.util.Properties
import java.util.{Random, Date}
import kafka.producer._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf


/**
 * Produce messages to kafka.
 *   <brokerList> is a list of kafka's blocks.
 *   <topics> is a kafka topics to consume from
 *   <keyPrefix> the prefix of key. If it is "stdin", you can input the message yoursel. format: <key,value>
 *   <keyNumber> the number of keys.
 *   <startValue> the start value of value
 *   <ValueNumber> the number of value.
 *   <interval> the interval between two messages
 *
 * Example:
 *   spark-submit --class com.yhd.ycache.magic.KafkaWordCountProducer   --master local \
 *   ./SSExample-0.0.1-SNAPSHOT-jar-with-dependencies.jar "lujian:9092" test a 26 -100 100 100
 */
object KafkaWordPairProducer {

  def main(args: Array[String]) {
    if (args.length < 7) {
      System.err.println("Usage args: <brokerList> <topic> " +
        "<key Prefix> <key number> <startValue> <value number> <interval>")
      System.exit(1)
    }

    val Array(brokers, topic, keyPrefix, keyNumber, startValue, valueNumber, interval) = args

    // Zookeeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // Send some messages
    while(true) {
      var str = ""
      if (keyPrefix == "stdin") {
        str = Console.readLine.trim
      } else {
        val message = (keyPrefix + "-" + (scala.util.Random.nextInt(keyNumber.toInt)).toString,
          scala.util.Random.nextInt(valueNumber.toInt) + startValue.toInt)
        str = f"${message._1},${message._2}\t"
        print(str)
      }
      if (str.length != 0 && str.contains(",")) {
        val messages = new KeyedMessage[String, String](topic, str)
        producer.send(messages)
        Thread.sleep(interval.toInt)
      }
    }
  }
}

/**
 * Consumes messages from one or more topics in Kafka and does stream joining.
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <topics> is a list of one or more kafka topics to consume from
 *   <bathInterval> the interval of bath in spark stream. see spark stream docs.
 *   <windows> is a range of time, and we process the messages in kafka topics., see spark stream docs.
 *   <slide> how many 'interval' the windows move, see spark stream docs.
 *
 * Example:
 *    spark-submit --class com.yhd.ycache.magic.SSExample  --master local[4] ./SSExample-0.0.1-SNAPSHOT-jar-with-dependencies.jar   \
 *    "jason4:2181/kafka-jason" test dest 60 4 2 2
 */
object SSExample {
  def main(args: Array[String]) {
    if (args.length<1){
      println("Error, usage: <zkQuorum> <topics1> <topics2> <window1> <window2> <bathInterval> <slide>")
      return
    }
    val startTime = new Date()

    val group = "jason-test"
    val Array(zkQuorum, topics1,topics2, window1, window2, bathInterval, slide) = args
    val sparkConf = new SparkConf().setAppName("SSExample")
    val ssc =  new StreamingContext(sparkConf, Seconds(bathInterval.toInt))
    ssc.checkpoint("checkpoint")

    val topicMap1 = topics1.split(",").map((_,1)).toMap
    val topicMap2 = topics2.split(",").map((_,1)).toMap
    val lines1 = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap1).map(_._2)
    val lines2 = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap2).map(_._2)
    val words1 = lines1.flatMap(_.split("\\s+")).filter(_.contains(",")).map(kv =>(kv.split(",")(0), kv.split(",")(1).toInt)).window(Seconds(window1.toInt), Seconds(slide.toInt))
    val words2 = lines2.flatMap(_.split("\\s+")).filter(_.contains(",")).map(kv =>(kv.split(",")(0), kv.split(",")(1).toInt)).window(Seconds(window2.toInt), Seconds(slide.toInt))
    val combineTemp = words1.cogroup(words2).map(kv => (kv._1,(kv._2._1.toArray,kv._2._2.toArray)))
    val combine = combineTemp.filter(kv => kv._2._1.length>0 && kv._2._2.length>0).map(kv => (kv._1, kv._2._1 ++ kv._2._2))
    val combineStr = combine.map(kv => (kv._1," -> \t[" + kv._2.mkString(",") + "]"))
    combineStr.print(10000)

    ssc.start()
    ssc.awaitTermination()

    val endTime = new Date()
    System.out.println("###used time: "+(endTime.getTime()-startTime.getTime())+"ms. ###")

  }
}
