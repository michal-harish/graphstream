package net.imagini.dxp.graphstream.connectedbsp

import java.nio.ByteBuffer
import java.util.Properties

import kafka.message.MessageAndOffset
import kafka.producer.KeyedMessage
import net.imagini.dxp.common.VidKafkaPartitioner
import org.apache.donut._
import org.apache.donut.memstore.MemStoreLogMap
import org.apache.donut.utils.logmap.ConcurrentLogHashMap

/**
 * Created by mharis on 14/09/15.
 */
class ConnectedBSPProcessingUnit(config: Properties, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  type MESSAGE = KeyedMessage[ByteBuffer, ByteBuffer]

  private val logmap = new ConcurrentLogHashMap(
    maxSizeInMb = config.getProperty("direct.memory.mb").toInt / totalLogicalPartitions - 128,
    segmentSizeMb = 100,
    compressMinBlockSize = 131070,
    indexLoadFactor = 0.87) {
    override def onEvictEntry(key: ByteBuffer): Unit = {
      /**
       * When the in-memory state overflows we also create a tombstone in the compacted state topic
       */
      produce(List(
        new KeyedMessage("graphstate", key, null.asInstanceOf[ByteBuffer]),
        new KeyedMessage("graphdelta", key, null.asInstanceOf[ByteBuffer])
      ))
    }
  }

  private val processor = new ConnectedBSPProcessor(minEdgeProbability = 0.75, new MemStoreLogMap(logmap))

  private val deltaProducer = kafkaUtils.createSnappyProducer[VidKafkaPartitioner](numAcks = 0, batchSize = 1000)

  private val stateProducer = kafkaUtils.createCompactProducer[VidKafkaPartitioner](numAcks = 0, batchSize = 200)

  override def onShutdown: Unit = {
    deltaProducer.close
    stateProducer.close
  }

  @volatile var ts = System.currentTimeMillis

  override def awaitingTermination {
    val period = (System.currentTimeMillis.toDouble - ts) / 1000
    val stateInPerSec = (processor.stateIn.get / period).toLong
    val deltaInPerSec = (processor.deltaIn.get / period).toLong
    val deltaOutPerSec = (processor.deltaOut.get / period).toLong
    println(s"graphdelta-input(${deltaInPerSec}/sec) invalid:${processor.invalid.get} evicted:${processor.stateEvict.get}" +
      s"missed:${processor.stateMiss.get}) => graphstate-input(${stateInPerSec}/sec) => output(${deltaOutPerSec}/sec)")
    processor.memstore.printStats(false)
    ts = System.currentTimeMillis
    processor.stateIn.set(0)
    processor.deltaIn.set(0)
    processor.stateEvict.set(0)
    processor.stateMiss.set(0)
    processor.deltaOut.set(0)

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

