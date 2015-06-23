
package com.yhd.ycache.magic

import java.util.Properties
import java.util.{Random, Date}
import kafka.producer._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext,DataFrame}
import Utils._
import scala.io.Source

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

object KafkaProducerFromFile{

  def main(args: Array[String]) {
    var cnt = 0
    if (args.length < 5) {
      System.err.println("Usage args: <brokerList> <topic> " +
        "<file> <interval(ms)> <repeat>")
      System.exit(1)
    }

    val Array(brokers, topic,file, interval,repeat) = args

    // Zookeeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    val source = Source.fromFile(file).getLines
    val lines = source.toArray

    // Send some messages
    while(true) {
      for (str <- lines) {
        if (str.length > 0) {
          val messages = new KeyedMessage[String, String](topic, str)
          producer.send(messages)
          Thread.sleep(interval.toInt)
        }
      }
      cnt += 1
      if (repeat.toLong > 0 && cnt > repeat.toLong)
        System.exit(0)
    }
  }
}


case class KV(k: String, v:Long)

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
object KafkaConsumer extends Serializable{
  def main(args: Array[String]) {
    if (args.length<1){
      println("Error, usage: <zkQuorum> <topics1> <topics2> <window1> <window2> <bathInterval> <slide>")
      return
    }
    val startTime = new Date()

    //val group = "jason-test"
    val Array(zkQuorum, topics1,topics2, window1, window2, bathInterval, slide, group) = args
    val sparkConf = new SparkConf().setAppName("SSExample")
    val ssc =  new StreamingContext(sparkConf, Seconds(bathInterval.toInt))
    ssc.checkpoint("checkpoint")
    val sqlContext = new SQLContext(ssc.sparkContext)
    import sqlContext.implicits._
    val sc = ssc.sparkContext
//
//    // Create an RDD
//    val people = sc.textFile("examples/src/main/resources/people.txt")
//
//    // The schema is encoded in a string
//    val schemaString = "name age"
//
//    // Import Spark SQL data types and Row.
//    import org.apache.spark.sql._
//
//    // Generate the schema based on the string of schema
//    val schema =
//      StructType(
//        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
//
//    // Convert records of the RDD (people) to Rows.
//    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
//
//    // Apply the schema to the RDD.
//    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
//
//    // Register the DataFrames as a table.
//    peopleDataFrame.registerTempTable("people")
//
//    // SQL statements can be run by using the sql methods provided by sqlContext.
//    val results = sqlContext.sql("SELECT name FROM people")
//
//    // The results of SQL queries are DataFrames and support all the normal RDD operations.
//    // The columns of a row in the result can be accessed by ordinal.
//    results.map(t => "Name: " + t(0)).collect().foreach(println)


    val topicMap1 = topics1.split(",").map((_,1)).toMap
    //val topicMap2 = topics2.split(",").map((_,1)).toMap
    val lines1 = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap1).map(_._2)
    //lines1.print(10000)
    //lines1.saveAsTextFiles("hdfs://heju/user/root/magic/kfk/test")
    //val lines2 = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap2).map(_._2)
    val words1 = lines1.flatMap(_.split("\\s+")).filter(_.contains(",")).map(kv =>(kv.split(",")(0), kv.split(",")(1).toInt)).window(Seconds(window1.toInt), Seconds(slide.toInt))
    //val words2 = lines2.flatMap(_.split("\\s+")).filter(_.contains(",")).map(kv =>(kv.split(",")(0), kv.split(",")(1).toInt)).window(Seconds(window2.toInt), Seconds(slide.toInt))
    //val combineTemp = words1.cogroup(words2).map(kv => (kv._1,(kv._2._1.toArray,kv._2._2.toArray)))
    //val combine = combineTemp.filter(kv => kv._2._1.length>0 && kv._2._2.length>0).map(kv => (kv._1, kv._2._1 ++ kv._2._2)).map(kv => (kv._1, kv._2.sum))
    val combine = words1
    //combine.print(10000)

    combine.map(kv=> KV(kv._1,kv._2)).foreachRDD(rdd =>{
      import scala.io.Source
      val source = Source.fromFile("/root/scala/sql.txt", "UTF-8").getLines
      val lines = source.toArray
      val sql = if (lines.length>0) lines(0) else "select * from kv"
      if (sql != None && sql.length>0 && rdd.count>0){
      println(">>>start of a rdd...")

      println("sql is " + sql)

      val df = rdd.toDF.registerTempTable("kv")
      val rt = sqlContext.sql(sql)
      rt.collect().map(_.mkString(",")).foreach(println)
      println("<<<end of a rdd...")
    }})



   // val combineStr = combine.map(kv => (kv._1," -> \t[" + kv._2.mkString(",") + "]"))
   // combineStr.print(10000)

    ssc.start()
    ssc.awaitTermination()

    val endTime = new Date()
    System.out.println("###used time: "+(endTime.getTime()-startTime.getTime())+"ms. ###")

  }


}

/**
 * A simple example using RandomForest to train a classifier and predict the result of new feature.
 *
 * step:
 * 1. Pick data from hive using Hivecontex;
 * 2. Transform the data to LablePoint format, which is used by RandomForest algorithm
 * 3. Split the data into training data and predicting date
 * 4. Train the classifier mode
 * 5. Predict the result using the predicting data
 * 6. Evaluate the preformance of the mode, e.g. Mean Squared Error
 * <sql> is a string of SQL statement
 * <useFraction> is fraction to split the data into training data and predicting data, e.g. 0.7 mean 70% is training data.
 * <numClasses_> is the number of class you try to training
 * <numTrees_> is the number of ther trees for the RandomForest algorithm
 * <impurity_> is the impurity for the RandomForest algorithm, e.g. 'gini' or 'entropy'
 * <maxDepth> is the maximum depth of the tree
 * <maxBins_> is the maximum number of bins used for splitting features(suggested value: 100)
 * <default_parallelism_> is how many task should be run at the same time.
 */
object Model extends Serializable{
  def main(args: Array[String]) {
    if (args.length<1){
      println("Error, usage: sql,useFraction, numClasses_,  numTrees_,impurity_, maxDepth_, maxBins_,default_parallelism_")
      return
    }
    val startTime = new Date()
    println(args)
    val Array(sql,useFraction, numClasses_,  numTrees_,impurity_, maxDepth_, maxBins_,default_parallelism_) = args
    val sparkConf = new SparkConf().setAppName("Mode Example")
    sparkConf.set("spark.default.parallelism",s"${default_parallelism_}")
    val sc = new SparkContext(sparkConf)  //{def }
    val hive = new HiveContext(sc)

    //get data by hive sql
    val rows = hive.sql(sql)

    // build the labeledPoint for classification
    val dataTemp = rows.map(r => {
      val arr = r.toSeq.toArray
      val label = string2Double(arr(r.length-1).toString)
      def fmap = (input: Any) => string2Double(input.toString)
      val feature = arr.slice(0,arr.length-1).map(fmap)
      LabeledPoint(label, Vectors.dense(feature))
    })
    println("check partition number")
    println("partition number:" + dataTemp.partitions.length)
    val data = dataTemp.repartition(Math.max(default_parallelism_.toInt,dataTemp.partitions.length))
//    val data = dataTemp
    println("partition number:" + data.partitions.length)

    val splits = data.randomSplit(Array(useFraction.toDouble, 1-useFraction.toDouble))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = numClasses_.toInt
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = numTrees_.toInt // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity =impurity_ // "gini"
    val maxDepth = maxDepth_.toInt
    val maxBins = maxBins_.toInt



    val model = RandomForest.trainClassifier(
      trainingData,
      numClasses,
      categoricalFeaturesInfo,
      numTrees,
      featureSubsetStrategy,
      impurity,
      maxDepth,
      maxBins)

    val endTime1 = new Date()
    System.out.println("###RandomForest.trainClassifier used time: "+(endTime1.getTime()-startTime.getTime())+"ms. ###")
//    def trainClassifier(
//                         input: RDD[LabeledPoint],
//                         numClasses: Int,
//                         categoricalFeaturesInfo: Map[Int, Int],
//                         numTrees: Int,
//                         featureSubsetStrategy: String,
//                         impurity: String,
//                         maxDepth: Int,
//                         maxBins: Int,
//                         seed: Int = Utils.random.nextInt()): RandomForestModel


    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    labelsAndPredictions.cache()
    println(labelsAndPredictions.take(200).mkString("\t"))
    println("Precision is: " + labelsAndPredictions.filter(item => item._1.toInt == item._2.toInt).count() * 1.0 / labelsAndPredictions.count())
    val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
    println("Test Mean Squared Error = " + testMSE)
    //println("Learned regression forest model:\n" + model.toDebugString)

    val endTime = new Date()
    System.out.println("###used time: "+(endTime.getTime()-startTime.getTime())+"ms. ###")

  }


}


