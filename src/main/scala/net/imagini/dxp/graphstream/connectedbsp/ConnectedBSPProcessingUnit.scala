package net.imagini.dxp.graphstream.connectedbsp

import java.net.URL
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import kafka.producer.KeyedMessage
import net.imagini.dxp.common.VidKafkaPartitioner
import org.apache.donut._
import org.apache.donut.memstore.MemStoreLogMap
import org.apache.donut.metrics.{Info, Throughput, Counter}
import org.apache.donut.utils.logmap.ConcurrentLogHashMap

/**
 * Created by mharis on 14/09/15.
 */
class ConnectedBSPProcessingUnit(
  config: Properties, trackingUrl: URL, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, trackingUrl, logicalPartition, totalLogicalPartitions, topics) {

  type MESSAGE = KeyedMessage[ByteBuffer, ByteBuffer]

  val evictions = new AtomicLong(0)

  private val logmap = new ConcurrentLogHashMap(
    maxSizeInMb = config.getProperty("direct.memory.mb").toInt / totalLogicalPartitions - 128,
    segmentSizeMb = 100,
    compressMinBlockSize = 131070,
    indexLoadFactor = 0.87) {
    /**
     * When the in-memory state overflows we also create a tombstone in the compacted state topic
     */
    override def onEvictEntry(key: ByteBuffer): Unit = {
      if (key == null || key.remaining <= 0) {
        throw new IllegalArgumentException("Key cannot be null or empty")
      }
      evictions.incrementAndGet
      //TODO test what is better in this situation: either sync producer with full zero-copy async producers
      //key is a sliced bytebuffer which may be re-used so we need to make a copy for the kafka async producer
      /*
      val keyCopy = ByteBuffer.wrap(ByteUtils.bufToArray(key))
      produce(List(
        new KeyedMessage("graphstate", keyCopy, null.asInstanceOf[ByteBuffer]),
        new KeyedMessage("graphdelta", keyCopy, null.asInstanceOf[ByteBuffer])
      ))
      */
      produce(List(
        new KeyedMessage("graphstate", key, null.asInstanceOf[ByteBuffer]),
        new KeyedMessage("graphdelta", key, null.asInstanceOf[ByteBuffer])
      ))
    }
  }

  private val processor = new ConnectedBSPProcessor(minEdgeProbability = 0.75, new MemStoreLogMap(logmap))

  private val deltaProducer = kafkaUtils.createSnappyProducer[VidKafkaPartitioner](async = false, numAcks = 0, batchSize = 1000)

  private val stateProducer = kafkaUtils.createCompactProducer[VidKafkaPartitioner]( async = false, numAcks = 0, batchSize = 200)

  override def onShutdown: Unit = {
    deltaProducer.close
    stateProducer.close
  }

  @volatile var ts = System.currentTimeMillis
  override def awaitingTermination {
    val period = (System.currentTimeMillis - ts)
    ts = System.currentTimeMillis
    sendMetric("gs:in/sec", classOf[Throughput], processor.stateIn.getAndSet(0) * 1000 / period)
    sendMetric("gs:evict/sec", classOf[Throughput], evictions.getAndSet(0) * 1000 / period)
    sendMetric("gs:invalid", classOf[Counter], processor.invalid.get)
    sendMetric("s:size", classOf[Counter], processor.memstore.size)
    sendMetric("s:memory.mb", classOf[Counter], processor.memstore.sizeInBytes / 1024 / 1024)
    sendMetric("s:extra", classOf[Info], s"<a title='${processor.memstore.stats(true).mkString("\n")}'>info</a>")
    sendMetric("d:msg/sec", classOf[Throughput], processor.deltaIn.getAndSet(0) * 1000 / period)
    sendMetric("d:msg/sec", classOf[Throughput], processor.deltaIn.getAndSet(0) * 1000 / period)
    sendMetric("d:miss", classOf[Counter], processor.miss.get)
    sendMetric("d:excess", classOf[Counter], processor.excess.get)
    sendMetric("o:msg/sec", classOf[Throughput], processor.deltaOut.getAndSet(0) * 1000 / period)
  }

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {

      case "graphstate" => new FetcherBootstrap(this, topic, partition, groupId) {
        def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          if (!isBooted) {
            val outputMessages = processor.bootState(messageAndOffset.message.key, messageAndOffset.message.payload)
            produce(outputMessages)
          }
        }
      }

      case "graphdelta" => new FetcherDelta(this, topic, partition, groupId) {
        override def handleMessage(envelope: MessageAndOffset): Unit = {
          val outputMessages = processor.processDeltaInput(envelope.message.key, envelope.message.payload)
          produce(outputMessages)
        }
      }
    }
  }

  def produce(outputMessages: List[MESSAGE]): Unit = {
    outputMessages.foreach(message =>
      message.topic match {
        case "graphdelta" => deltaProducer.send(message)
        case "graphstate" => stateProducer.send(message)
      })
  }


}

