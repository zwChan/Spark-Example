# Several very simple Spark Examples.

## List of examples

### 0. Produce messages to kafka.
#### Args:
    <brokerList> is a list of kafka's blocks.

    <topics> is a kafka topics to consume from

    <keyPrefix> the prefix of key. If it is "stdin", you can input the message yoursel. format: <key,value>

    <keyNumber> the number of keys.

    <startValue> the start value of value

    <ValueNumber> the number of value.

    <interval> the interval between two messages

#### Example:

    spark-submit --class com.yhd.ycache.magic.KafkaWordCountProducer   --master local \

    ./SSExample-1.0.0-jar-with-dependencies.jar "lujian:9092" test a 26 -100 100 100

### 1. Consumes messages from one or more topics in Kafka and does stream joining.

    <zkQuorum> is a list of one or more zookeeper servers that make quorum

    <topics> is a list of one or more kafka topics to consume from

    <bathInterval> the interval of bath in spark stream. see spark stream docs.

    <windows> is a range of time, and we process the messages in kafka topics., see spark stream docs.

    <slide> how many 'interval' the windows move, see spark stream docs.

####  Example:

     spark-submit --class com.yhd.ycache.magic.SSExample  --master local[4] ./SSExample-1.0.0-jar-with-dependencies.jar   \

     "jason4:2181/kafka-jason" test dest 60 4 2 2

### Used commands of above two examples
  ** Assuming that you has two topic 'test' and 'dest' on kafka. One of the block id is 'lujian:9092' **

  ** and zookeeper path of kafka is 'jason4:2181/kafka-jason' **

 - Produce messages of key-value to the topic 'test'
``
spark-submit --class com.yhd.ycache.magic.KafkaWordPairProducer --master local ./SSExample-1.0.0-jar-with-dependencies.jar "lujian:9092" test aaa 26  -100 100 100
``
 - Produce messages of key-value to the topic 'dest'
``
spark-submit --class com.yhd.ycache.magic.KafkaWordPairProducer --master local ./SSExample-1.0.0-jar-with-dependencies.jar "lujian:9092" dest aaa 26  100 100 100
``
 - join above two streams in Spark stream.
``
spark-submit --class com.yhd.ycache.magic.SSExample  --master local[4] ./SSExample-1.0.0-jar-with-dependencies.jar    "jason4:2181/kafka-jason" test dest 60 4 2 2
``
### 2. RandomForest classifier training

 A simple example using RandomForest to train a classifier and predict the result of new feature.
 
 step:
  1. Pick data from hive using Hivecontex;
  2. Transform the data to LablePoint format, which is used by RandomForest algorithm
  3. Split the data into training data and predicting date
  4. Train the classifier mode
  5. Predict the result using the predicting data
  6. Evaluate the preformance of the mode, e.g. Mean Squared Error
 
#### Args:
    <sql> is a string of SQL statement
    <useFraction> is fraction to split the data into training data and predicting data, e.g. 0.7 mean 70% is training data.
    <numClasses_> is the number of class you try to training
    <numTrees_> is the number of ther trees for the RandomForest algorithm
    <impurity_> is the impurity for the RandomForest algorithm, e.g. 'gini' or 'entropy'
    <maxDepth> is the maximum depth of the tree
    <maxBins_> is the maximum number of bins used for splitting features(suggested value: 100)
    <default_parallelism_> is how many task should be run at the same time.    
  
#### exmaple:
    spark-submit --class com.yhd.ycache.magic.Model /root/scala/SSExample-1.0.0.jar "select  buy_days,  \
    first_ordr_days,  creat_days,  phone_flag,  last_ordr_days,  shanghai_flag,  pm_wght,  shpmt_amt,  mphone_flag,  \
    import_milk_flag,  ordr_num_y,  categ_lvl3_num_y,  sku_num_y,  pm_num_y,  discount_pct_y,  shpmt_amt_y,  \
    import_food_ordr_y,  kitch_bath_ordr_y,  kitch_bath_pct_y,  clothes_pct_y,  visit_day_num,  elpsed_time, \
    categ_page_pv,  colct_shop_num,  non_foods_categ_num,  non_foods_kw_num,  foods_categ_num,  sku_vis_num_p3,  \
    ordr_num_p3,  ordr_num_p2,  ordr_num_p1,  loss_3mon_flag  from tmp_dev.cust_loss_unit_1_2014 limit 10000" \
    0.7 2 1000 gini 8 100 36