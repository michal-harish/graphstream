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
    val output = List.newBuilder[MESSAGE]
    memstore.get(key, b => b) match {
      case None => {
        bspMiss.incrementAndGet
        memstore.put(key, payload)
        output += new KeyedMessage("graphstate", key, payload)
      }
      case Some(null) => bspOverflow.incrementAndGet

      case Some(serializedState) => {
        val (iteration, inputEdges) = BSPMessage.decodePayload(payload)
        val state = BSPMessage.decodePayload(serializedState)._2
        var evicted = false

        //remove evicted from the state
        val it0 = inputEdges.entrySet.iterator
        while (it0.hasNext) {
          val i = it0.next
          val ik = i.getKey
          val iv = i.getValue
          if (iv.probability == 0) {
            state.remove(ik)
            evicted = true
          }
        }

        //remove existing from the input
        val it1 = state.entrySet.iterator
        while (it1.hasNext) {
          val e = it1.next
          val vid = e.getKey
          val ee = e.getValue
          val i = inputEdges.get(vid)
          if (i != null && (i.probability == 0 || i.probability <= ee.probability)) {
            inputEdges.remove(vid)
          }
        }

        if (inputEdges.size > MAX_EDGES) {
          //evict offending key and remove all the edges pointing to it
          val nullPayload = null.asInstanceOf[ByteBuffer]
          memstore.put(key, nullPayload)
          output += new KeyedMessage("graphdelta", key, nullPayload)
          output += new KeyedMessage("graphstate", key, nullPayload)
          val evictDest = BSPMessage.decodeKey(key)
          val evictProps = Edge(Edge.VENDOR_CODE_UNKNOWN, 0.0, System.currentTimeMillis)
          output ++= propagateEdge(iteration, evictDest, evictProps, state)
        } else if (inputEdges.size > 0 || evicted) {
          if (iteration < MAX_ITERATIONS && inputEdges.size > 0) {
            output ++= propagateEdges(iteration, inputEdges, state)
            output ++= propagateEdges(iteration, state, inputEdges)
          }
          state.putAll(inputEdges)
          val payload = BSPMessage.encodePayload((iteration, state))
          memstore.put(key, payload)
          output += new KeyedMessage("graphstate", key, payload)
        }
      }
    }
    val outputMessages = output.result
    outputMessages
  }

  /**
   * Propagate edges to each of the targets if the probability doesn't fall below 0.75 in the process.
   * @param edges
   * @param dest
   * @return
   */
  private def propagateEdges(iteration: Int, edges: java.util.Map[Vid, Edge], dest: java.util.Map[Vid, Edge]): Iterable[MESSAGE] = {
    val result = List.newBuilder[MESSAGE]
    val it = edges.entrySet.iterator
    while (it.hasNext) {
      val i = it.next
      result ++= propagateEdge(iteration, i.getKey, i.getValue, dest)
    }
    result.result
  }

  private def propagateEdge(iteration: Int, vid: Vid, props: Edge, dest: java.util.Map[Vid, Edge]): Iterable[MESSAGE] = {
    val it = dest.entrySet.iterator
    val result = List.newBuilder[MESSAGE]
    while (it.hasNext) {
      val d = it.next
      val propagateVid = d.getKey
      if (propagateVid != vid) {
        val destEdge = d.getValue
        val propagateEdge = Edge(props.vendorCode, props.probability * destEdge.probability, math.max(destEdge.ts, props.ts))
        if (propagateEdge.probability == 0 || propagateEdge.probability >= minEdgeProbability) {
          val payload = BSPMessage.encodePayload((iteration + 1).toByte, (vid, propagateEdge))
          result += new KeyedMessage("graphdelta", BSPMessage.encodeKey(propagateVid), payload)
        }
      }
    }
    result.result
  }

}
