package net.imagini.dxp.graphstream.connectedbsp

import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import net.imagini.dxp.common.VidKafkaPartitioner
import org.apache.donut._

/**
 * Created by mharis on 14/09/15.
 */
class ConnectedBSPProcessingUnit(config: Properties, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  private val processor = new ConnectedBSPProcessor(maxStateSizeMb = config.getProperty("state.memory.mb").toInt, minEdgeProbability = 0.75)

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
    println(
      s"=> graphdelta-input(${deltaInPerSec}/sec) evicted ${processor.stateEvict.get}  missed ${processor.stateMiss.get})" +
        s"=> graphstate-input(${stateInPerSec}/sec) " +
        s"=> output(${deltaOutPerSec}/sec) " +
        s"=> state.size = " + processor.state.size + ", state.memory = " + processor.state.minSizeInBytes / 1024 / 1024 + " Mb"
    )
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
            processor.bootState(messageAndOffset.message.key, messageAndOffset.message.payload)
          }
        }
      }

      case "graphdelta" => new FetcherDelta(this, topic, partition, groupId) {
        override def handleMessage(envelope: MessageAndOffset): Unit = {
          val outputMessages = processor.processDeltaInput(envelope.message.key, envelope.message.payload)
          outputMessages.foreach(message =>
            message.topic match {
              case "graphdelta" => deltaProducer.send(message)
              case "graphstate" => stateProducer.send(message)
            })
        }
      }
    }
  }

}

