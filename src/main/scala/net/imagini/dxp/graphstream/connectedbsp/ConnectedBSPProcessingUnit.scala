package net.imagini.dxp.graphstream.connectedbsp

import java.util.Properties

import kafka.message.MessageAndOffset
import net.imagini.dxp.common.VidKafkaPartitioner
import org.apache.donut._

/**
 * Created by mharis on 14/09/15.
 */
class ConnectedBSPProcessingUnit(config: Properties, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  private val processor = new ConnectedBSPProcessor(
    maxStateSizeMb = config.getProperty("direct.memory.mb").toInt / totalLogicalPartitions, minEdgeProbability = 0.75)

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
    println(s"memstore.size = " + processor.memstore.size + "  memstore.mb = "
      + processor.memstore.sizeInBytes / 1024 / 1024 + " Mb  memstore.c = " + processor.memstore.compressRatio)
//    println(s"altstore.size = " + processor.altstore.size + "  altstore.mb = "
//      + processor.altstore.sizeInBytes / 1024 / 1024 + " Mb  altstore.c = " + processor.altstore.compressRatio)
    println(s"")

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

  private def produce(outputMessages: List[processor.MESSAGE]) = {
    outputMessages.foreach(message =>
      message.topic match {
        case "graphdelta" => deltaProducer.send(message)
        case "graphstate" => stateProducer.send(message)
      })
  }


}

