package com.ippontech.kafka

import com.ippontech.kafka.stores.{OffsetsStore, ZooKeeperOffsetsStore}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.api.java.{JavaDStream, JavaStreamingContext}

object KafkaSourcePythonHelper {

  def kafkaStream(jssc: JavaStreamingContext, brokers: String, offsetsStore: OffsetsStore,
                  topic: String): JavaDStream[(String, String)] = {
    val dstream = KafkaSource.kafkaStream[String, String, StringDecoder, StringDecoder](jssc.ssc, brokers, offsetsStore, topic)
    val jdstream = new JavaDStream(dstream)
    jdstream
  }

  def kafkaStream(jssc: JavaStreamingContext, brokers: String, zkHosts: String, zkPath: String,
                  topic: String): JavaDStream[(String, String)] = {
    val offsetsStore = new ZooKeeperOffsetsStore(zkHosts, zkPath)
    val dstream = KafkaSource.kafkaStream[String, String, StringDecoder, StringDecoder](jssc.ssc, brokers, offsetsStore, topic)
    val jdstream = new JavaDStream(dstream)
    jdstream
  }

}
