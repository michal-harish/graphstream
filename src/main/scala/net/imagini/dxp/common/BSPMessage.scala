package net.imagini.dxp.common

import java.nio.ByteBuffer
import java.util

import io.amient.utils.ByteUtils

/**
 * Created by mharis on 10/09/15.
 */
object BSPMessage {

  def encodeKey(key: Vid): ByteBuffer = ByteBuffer.wrap(key.bytes)

  def decodeKey(key: ByteBuffer): Vid = Vid(ByteUtils.bufToArray(key))

  def encodePayload(payload: (Byte, Map[Vid, Edge])): ByteBuffer = {
    val (iter, edges) = payload
    val len = edges.foldLeft(1 + 2)((l, item) => l + 8 + 1 + item._1.bytes.length + 4)
    val result = ByteBuffer.allocate(len)
    result.put(iter)
    result.putShort(edges.size.toShort)
    edges.foreach { case (vid, edge) => {
      result.putLong(edge.ts)
      result.put(vid.bytes.length.toByte)
      result.put(vid.bytes)
      result.put(edge.bytes)
    }
    }
    result.flip
    result
  }

  def decodePayload(payload: ByteBuffer): (Byte, Map[Vid, Edge]) = {
    if (payload == null) {
      null
    } else {
      val p = payload.position
      val iter = payload.get
      val size = payload.getShort.toInt
      val result = (iter, (for (i <- (1 to size)) yield {
        val ts = payload.getLong
        val vidBytes = new Array[Byte](payload.get())
        payload.get(vidBytes)
        val vid = Vid(vidBytes)
        val edgeBytes = new Array[Byte](4)
        payload.get(edgeBytes)
        val edge = Edge.applyVersion(edgeBytes, ts)
        (vid, edge)
      }).toMap)
      payload.position(p)
      result
    }
  }

}
