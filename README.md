# A very simple Spark Steaming Example to join two streams.

## How to use
/**
  Produce messages to kafka.
    <brokerList> is a list of kafka's blocks.
    <topics> is a kafka topics to consume from
    <keyPrefix> the prefix of key. If it is "stdin", you can input the message yoursel. format: <key,value>
    <keyNumber> the number of keys.
    <startValue> the start value of value
    <ValueNumber> the number of value.
    <interval> the interval between two messages

  Example:
    spark-submit --class com.yhd.ycache.magic.KafkaWordCountProducer   --master local \
    ./SSExample-0.0.1-SNAPSHOT-jar-with-dependencies.jar "lujian:9092" test a 26 -100 100 100
 */


/**
  Consumes messages from one or more topics in Kafka and does stream joining.
    <zkQuorum> is a list of one or more zookeeper servers that make quorum
    <topics> is a list of one or more kafka topics to consume from
    <bathInterval> the interval of bath in spark stream. see spark stream docs.
    <windows> is a range of time, and we process the messages in kafka topics., see spark stream docs.
    <slide> how many 'interval' the windows move, see spark stream docs.

  Example:
     spark-submit --class com.yhd.ycache.magic.SSExample  --master local[4] ./SSExample-0.0.1-SNAPSHOT-jar-with-dependencies.jar   \
     "jason4:2181/kafka-jason" test dest 60 4 2 2
 */

## Use example
  ** Assuming that you has two topic 'test' and 'dest' on kafka. One of the block id is 'lujian:9092' **
  ** and zookeeper path of kafka is 'jason4:2181/kafka-jason' **

 - Produce messages of key-value to the topic 'test'
'''
spark-submit --class com.yhd.ycache.magic.KafkaWordPairProducer --master local ./SSExample-0.0.1-SNAPSHOT-jar-with-dependencies.jar "lujian:9092" test aaa 26  -100 100 100
'''
 - Produce messages of key-value to the topic 'dest'
 '''
spark-submit --class com.yhd.ycache.magic.KafkaWordPairProducer --master local ./SSExample-0.0.1-SNAPSHOT-jar-with-dependencies.jar "lujian:9092" dest aaa 26  100 100 100
'''
 - join above two streams in Spark stream.
 '''
spark-submit --class com.yhd.ycache.magic.SSExample  --master local[4] ./SSExample-0.0.1-SNAPSHOT-jar-with-dependencies.jar    "jason4:2181/kafka-jason" test dest 60 4 2 2
'''