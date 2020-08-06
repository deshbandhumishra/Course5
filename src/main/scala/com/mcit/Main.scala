package com.mcit

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}
import org.apache.log4j.{LogManager, Logger}
object Main extends App{

  val logger:Logger = LogManager.getLogger("Main")

    logger.info("Creating Kafka Producer...");

  val props = new Properties

  props.put(ProducerConfig.CLIENT_ID_CONFIG, "HelloProducer")
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[Integer, String](props)

  logger.info("Start sending messages...")
  for (i <- 1 to 10) {
    logger.info("====================Simple Massage " + i)
    producer.send(new ProducerRecord[Integer, String]("Students", i, "Simple Message-" + i))
  }

  logger.info("Finished - Closing Kafka Producer.")
  producer.close()













  //============================================================================
   /* val kafkaParams = Map[String, String](
    //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "172.16.129.58:9092",
    //192.168.43.126/24
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "bix",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
  println("====================")
  val topic = "stop_times"
  val spark: SparkSession = SparkSession.builder().appName("Sample Spark SQL").master("local[*]").getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams))

  val stoptimes: DStream[String] = stream.map(record => record.value())
  stoptimes.foreachRDD(microRdd => {
    import spark.implicits._
    val df = microRdd
      .map(_.split(",", -1))
      .map(x => StopTimes(x(0), x(1), x(2), x(3), x(4)))
      .toDF
    df.show(3)
    df.createOrReplaceTempView("stoptimes_view")
   })*/
}