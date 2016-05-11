package com.ippontech.kafka

import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

object KafkaSource extends LazyLogging {

  // Kafka input stream
  def kafkaStream(ssc: StreamingContext, brokers: String, zkHosts: String, zkPath: String,
                  topic: String): InputDStream[(String, String)] = {
    val topics = Set(topic)
    val kafkaParams = Map("metadata.broker.list" -> brokers)

    val zkClient = new ZkClient(zkHosts, 10000, 10000, ZKStringSerializer)
    val storedOffsets = readOffsets(zkClient, zkPath, topic)
    val kafkaStream = storedOffsets match {
      case None =>
        // start from the latest offsets
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(fromOffsets) =>
        // start from previously saved offsets
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,
          (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }

    // save the offsets
    kafkaStream.foreachRDD(rdd => saveOffsets(zkClient, zkPath, rdd))

    kafkaStream
  }

  // Read the previously saved offsets from Zookeeper
  private def readOffsets(zkClient: ZkClient, zkPath: String, topic: String): Option[Map[TopicAndPartition, Long]] = {
    logger.info("Reading offsets from Zookeeper")
    val stopwatch = new Stopwatch()

    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)

    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        logger.debug(s"Read offset ranges: ${offsetsRangesStr}")

        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
          .toMap

        logger.info("Done reading offsets from Zookeeper. Took " + stopwatch)

        Some(offsets)
      case None =>
        logger.info("No offsets found in Zookeeper. Took " + stopwatch)
        None
    }
  }

  // Save the offsets back to Zookeeper
  //
  // IMPORTANT: We're not saving the offset immediately but instead save the offset from the previous batch. This is
  // because the extraction of the offsets has to be done at the beginning of the stream processing, before the real
  // logic is applied. Instead, we want to save the offsets once we have successfully processed a batch, hence the
  // workaround.
  private def saveOffsets(zkClient: ZkClient, zkPath: String, rdd: RDD[_]): Unit = {
    logger.info("Saving offsets to Zookeeper")
    val stopwatch = new Stopwatch()

    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetsRanges.foreach(offsetRange => logger.debug(s"Using ${offsetRange}"))

    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    logger.debug(s"Writing offsets to Zookeeper: ${offsetsRangesStr}")
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)

    logger.info("Done updating offsets in Zookeeper. Took " + stopwatch)
  }

  // very simple stop watch to avoid using Guava's one
  class Stopwatch {
    private val start = System.currentTimeMillis()

    override def toString() = (System.currentTimeMillis() - start) + " ms"
  }

}
