package net.imagini.graphstream.connectedbsp

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import kafka.producer.KeyedMessage
import net.imagini.graphstream.common.{Edge, Vid, BSPMessage}
import org.apache.donut._
import org.apache.donut.memstore.{MemStoreMemDb, MemStore}

/**
 * Created by mharis on 14/09/15.
 */
class ConnectedBSPProcessUnit(config: Properties, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  val bspIn = new AtomicLong(0)
  val bspEvicted = new AtomicLong(0)
  val bspMiss = new AtomicLong(0)
  val bspUpdated = new AtomicLong(0)
  val stateIn = new AtomicLong(0)

  private val localState: MemStore = new MemStoreMemDb(1024 * 14, 1000000)//new LocalStorage1(5000000)

  private val graphstreamProducer = kafkaUtils.createSnappyProducer[KafkaRangePartitioner](numAcks = 0, batchSize = 1000)

  private val stateProducer = kafkaUtils.createCompactProducer[KafkaRangePartitioner](numAcks = 0, batchSize = 200)

  override def onShutdown: Unit = graphstreamProducer.close

  override def awaitingTermination {
    println(
      s"=> graphstream(${bspIn.get} - evicted ${bspEvicted.get} + missed ${bspMiss.get} + hit ${bspUpdated.get}) " +
        s"=> graphstate(${stateIn.get}) " +
        s"=> state.size = " + localState.size + ", state.memory = " + localState.minSizeInBytes / 1024 / 1024 + " Mb"
    )
  }

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {

      case "graphstate" => new FetcherBootstrap(this, topic, partition, groupId) {
        def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          localState.put(messageAndOffset.message.key, messageAndOffset.message.payload)
          stateIn.incrementAndGet
        }
      }

      case "graphstream" => new FetcherDelta(this, topic, partition, groupId) {
        val MAX_ITER = 5
        private val MAX_EDGES = 99

        override def handleMessage(envelope: MessageAndOffset): Unit = {
          bspIn.incrementAndGet
          val key = envelope.message.key
          localState.get(key) match {
            case None => {
              bspMiss.incrementAndGet
              localState.put(key, envelope.message.payload)
              stateProducer.send(new KeyedMessage("graphstate", key, envelope.message.payload))
            }
            case Some(null) => bspEvicted.incrementAndGet
            case Some(previousState) => {
              bspUpdated.incrementAndGet
              val payload = envelope.message.payload
              val (iteration, inputEdges) = BSPMessage.decodePayload(payload.array, payload.arrayOffset)
              val existingEdges = BSPMessage.decodePayload(previousState)._2
              val additionalEdges = inputEdges.filter(n => !existingEdges.contains(n._1))
              val newEdges = existingEdges ++ additionalEdges
              if (newEdges.size > MAX_EDGES) {
                localState.put(key, null.asInstanceOf[Array[Byte]])
                stateProducer.send(new KeyedMessage("graphstate", key, null))
              } else {
                val newState = BSPMessage.encodePayload((iteration, newEdges))
                if (iteration < MAX_ITER) {
                  propagateEdges(iteration, newEdges, existingEdges)
                  propagateEdges(iteration, existingEdges, newEdges)
                }
                localState.put(key, newState)
                stateProducer.send(new KeyedMessage("graphstate", key, ByteBuffer.wrap(newState)))
              }
            }
          }
        }

        /**
         * Propagate edges to each of the targets if the probability doesn't fall below 0.75 in the process.
         * @param edges
         * @param targets
         * @return
         */
        private def propagateEdges(iteration: Int, edges: Map[Vid, Edge], targets: Map[Vid, Edge]) = {
          targets.foreach { case (targetVid, targetEdge) => {
            val propagateEdges = edges.mapValues (edge => {
              Edge(edge.vendorCode, edge.probability * targetEdge.probability, edge.ts)
            }).filter { case (vid, props) => vid != targetVid && props.probability > 0.75 }
            val key = ByteBuffer.wrap(BSPMessage.encodeKey(targetVid))
            val payload = ByteBuffer.wrap(BSPMessage.encodePayload(((iteration + 1).toByte, propagateEdges)))
            graphstreamProducer.send(new KeyedMessage("graphstream", key, payload))
          }}
        }
      }

    }
  }

}

