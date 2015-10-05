package net.imagini.dxp.graphstream.connectedbsp

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import kafka.producer.KeyedMessage
import net.imagini.dxp.common.{Vid, Edge, BSPMessage}
import org.apache.donut.memstore.{MemStoreLogMap, MemStore, MemStoreMemDb}

/**
 * Created by mharis on 26/09/15.
 *
 * ConnectedBSPProcessor - this processor is contains the logic of the streaming bsp algorithm and is completely
 * isolated from the consumer and producer streams by returning set of messages that are testable without any
 * connections or bootstrap state.
 */
class ConnectedBSPProcessor(maxStateSizeMb: Int, minEdgeProbability: Double) {

  type MESSAGE = KeyedMessage[ByteBuffer, ByteBuffer]
  val MAX_ITERATIONS = 3
  private val MAX_EDGES = 99

  val state: MemStore = new MemStoreMemDb(maxStateSizeMb)

  val stateIn = new AtomicLong(0)
  val deltaIn = new AtomicLong(0)
  val stateEvict = new AtomicLong(0)
  val stateMiss = new AtomicLong(0)
  val deltaOut = new AtomicLong(0)

  def bootState(key: ByteBuffer, payload: ByteBuffer): Unit = {
    state.put(key, payload)
    stateIn.incrementAndGet
  }

  /**
   * @param key
   * @param payload =
   * @return list of messages to produce
   */
  def processDeltaInput(key:ByteBuffer, payload: ByteBuffer): List[MESSAGE] = {
    deltaIn.incrementAndGet
    val output = List.newBuilder[MESSAGE]
    state.get(key, b => b) match {
      case None => {
        stateMiss.incrementAndGet
        output += updateState(key, payload)
      }
      case Some(null) => stateEvict.incrementAndGet

      case Some(previousState) => {
        val (iteration, inputEdges) = BSPMessage.decodePayload(payload)
        val existingEdges = BSPMessage.decodePayload(previousState)._2

        val evictedEdges = inputEdges.filter{
          case (inDest, inProps) => inProps.probability == 0 && existingEdges.contains(inDest)}.map(_._1)

        val additionalEdges = inputEdges.filter {
          case (inDest, inProps) => inProps.probability > 0 && !existingEdges.exists {
            case (exDest, exProps) => exDest == inDest && exProps.probability >= inProps.probability}}

        val newState = existingEdges ++ additionalEdges -- evictedEdges
        if (newState.size > MAX_EDGES) {
          //evict offending key and remove all the edges pointing to it
          val evictDest = BSPMessage.decodeKey(key)
          val evictEdge = Edge(Edge.VENDOR_CODE_UNKNOWN, 0.0, System.currentTimeMillis)
          output ++= propagateEdges(iteration, Map(evictDest -> evictEdge), existingEdges)
          output += updateState(key, null.asInstanceOf[ByteBuffer])
        } else if (additionalEdges.size > 0 || evictedEdges.size >0) {
          if (iteration < MAX_ITERATIONS && additionalEdges.size > 0) {
            output ++= propagateEdges(iteration, newState, existingEdges)
            output ++= propagateEdges(iteration, existingEdges, newState)
          }
          output += updateState(key, BSPMessage.encodePayload((iteration, newState)))
        }
      }
    }
    val outputMessages = output.result
    deltaOut.addAndGet(outputMessages.size)
    outputMessages
  }

  /**
   * Propagate edges to each of the targets if the probability doesn't fall below 0.75 in the process.
   * @param edges
   * @param dest
   * @return
   */
  private def propagateEdges(iteration: Int, edges: Map[Vid, Edge], dest: Map[Vid, Edge]): Iterable[MESSAGE] = {
    dest.flatMap { case (destVid, destEdge) => {
      val propagateEdges = edges.mapValues(edge => {
        Edge(edge.vendorCode, edge.probability * destEdge.probability, math.max(destEdge.ts, edge.ts))
      }).filter { case (vid, props) => vid != destVid && (props.probability == 0 || props.probability >= minEdgeProbability) }
      if (propagateEdges.size == 0) {
        Seq()
      } else {
        val destKey = BSPMessage.encodeKey(destVid)
        val payload = BSPMessage.encodePayload(((iteration + 1).toByte, propagateEdges))
        Seq(new KeyedMessage("graphdelta", destKey, payload))
      }
    }}
  }

  private def updateState(key: ByteBuffer, payload: ByteBuffer): MESSAGE = {
    state.put(key, payload)
    new KeyedMessage("graphstate", key, payload)
  }

}
