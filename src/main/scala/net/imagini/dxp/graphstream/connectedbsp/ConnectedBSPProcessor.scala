package net.imagini.dxp.graphstream.connectedbsp

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import io.amient.donut.memstore.MemStore
import kafka.producer.KeyedMessage
import net.imagini.dxp.common.{BSPMessage, Edge, Vid}
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 26/09/15.
 *
 * ConnectedBSPProcessor - this processor is contains the logic of the streaming bsp algorithm and is completely
 * isolated from the consumer and producer streams by returning set of messages that are testable without any
 * connections or bootstrap state.
 */
class ConnectedBSPProcessor(minEdgeProbability: Double, val memstore: MemStore) {

  type MESSAGE = KeyedMessage[ByteBuffer, ByteBuffer]

  private val log = LoggerFactory.getLogger(classOf[ConnectedBSPProcessor])

  val MAX_ITERATIONS = 3
  private val MAX_EDGES = 99

  val bspOverflow = new AtomicLong(0)
  val bspMiss = new AtomicLong(0)

  def bootState(msgKey: ByteBuffer, payload: ByteBuffer): Unit = {
    try {
      memstore.put(msgKey, payload)
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Throwable => throw new IllegalArgumentException("Invalid Key ByteBuffer " + msgKey, e)
    }
  }

  /**
   * @param key
   * @param payload =
   * @return list of messages to produce
   */
  def processDeltaInput(key: ByteBuffer, payload: ByteBuffer): List[MESSAGE] = {
    if (payload == null) {
      //eviction message was generated here, no need to process more
      return List()
    }
    try {
      val output = List.newBuilder[MESSAGE]
      memstore.get(key, b => b) match {
        case None => {
          bspMiss.incrementAndGet
          memstore.put(key, payload)
          output += new KeyedMessage("graphstate", key, payload)
        }
        case Some(null) => bspOverflow.incrementAndGet

        case Some(previousState) => {
          val (iteration, inputEdges) = BSPMessage.decodePayload(payload)
          val existingEdges = BSPMessage.decodePayload(previousState)._2

          val evictedEdges = inputEdges.filter {
            case (inDest, inProps) => inProps.probability == 0 && existingEdges.contains(inDest)
          }.map(_._1)

          val additionalEdges = inputEdges.filter {
            case (inDest, inProps) => inProps.probability > 0 && !existingEdges.exists {
              case (exDest, exProps) => exDest == inDest && exProps.probability >= inProps.probability
            }
          }

          val newState = existingEdges ++ additionalEdges -- evictedEdges
          if (newState.size > MAX_EDGES) {
            //evict offending key and remove all the edges pointing to it
            val nullPayload = null.asInstanceOf[ByteBuffer]
            memstore.put(key, nullPayload)
            output += new KeyedMessage("graphdelta", key, nullPayload)
            output += new KeyedMessage("graphstate", key, nullPayload)
            val evictDest = BSPMessage.decodeKey(key)
            val evictEdge = Edge(Edge.VENDOR_CODE_UNKNOWN, 0.0, System.currentTimeMillis)
            output ++= propagateEdges(iteration, Map(evictDest -> evictEdge), existingEdges)
          } else if (additionalEdges.size > 0 || evictedEdges.size > 0) {
            if (iteration < MAX_ITERATIONS && additionalEdges.size > 0) {
              output ++= propagateEdges(iteration, newState, existingEdges)
              output ++= propagateEdges(iteration, existingEdges, newState)
            }
            val payload = BSPMessage.encodePayload((iteration, newState))
            memstore.put(key, payload)
            output += new KeyedMessage("graphstate", key, payload)
          }
        }
      }
      val outputMessages = output.result
      outputMessages
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Throwable => throw new IllegalStateException("Invalid Key ByteBuffer " + key, e)
    }
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
    }
    }
  }

}
