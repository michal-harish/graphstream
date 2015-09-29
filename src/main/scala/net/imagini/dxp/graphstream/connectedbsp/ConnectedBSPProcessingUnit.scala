package net.imagini.dxp.graphstream.connectedbsp

import java.util.Properties

import kafka.message.MessageAndOffset
import org.apache.donut._

/**
 * Created by mharis on 14/09/15.
 */
class ConnectedBSPProcessingUnit(config: Properties, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  private val processor = new ConnectedBSPProcessor(maxStateSizeMb = 1024 * 8, minEdgeProbability = 0.75)

  private val deltaProducer = kafkaUtils.createSnappyProducer[KafkaRangePartitioner](numAcks = 0, batchSize = 1000)

  private val stateProducer = kafkaUtils.createCompactProducer[KafkaRangePartitioner](numAcks = 0, batchSize = 200)

  override def onShutdown: Unit = {
    deltaProducer.close
    stateProducer.close
  }

  override def awaitingTermination {
    println(
      s"=> graphdelta(${processor.bspIn.get} - evicted ${processor.bspEvicted.get} + missed ${processor.bspMiss.get} + hit ${processor.bspUpdated.get}) " +
        s"=> graphstate(${processor.stateIn.get}) " +
        s"=> state.size = " + processor.state.size + ", state.memory = " + processor.state.minSizeInBytes / 1024 / 1024 + " Mb"
    )
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

