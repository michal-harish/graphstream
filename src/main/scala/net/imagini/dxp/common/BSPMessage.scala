package net.imagini.dxp.common

import java.nio.ByteBuffer

import io.amient.utils.ByteUtils

/**
 * Created by mharis on 10/09/15.
 */
object BSPMessage {

  def encodeKey(key: Vid): ByteBuffer = ByteBuffer.wrap(key.bytes)

  def decodeKey(key: ByteBuffer): Vid = Vid(ByteUtils.bufToArray(key))

  def encodePayload(payload: (Byte, java.util.Map[Vid, Edge])): ByteBuffer = {
    val (iter, edges) = payload
    var len = 3
    val it = edges.entrySet.iterator
    while (it.hasNext) {
      len += (8 + 1 + it.next.getKey.bytes.length + 4)
    }
    val result = ByteBuffer.allocate(len)
    result.put(iter)
    result.putShort(edges.size.toShort)
    val it2 = edges.entrySet.iterator
    while (it2.hasNext) {
      val i = it2.next
      val vid = i.getKey
      val edge = i.getValue
      result.putLong(edge.ts)
      result.put(vid.bytes.length.toByte)
      result.put(vid.bytes)
      result.put(edge.bytes)
    }
    result.flip
    result
  }

  def encodePayload(iter: Byte, edge: (Vid, Edge)): ByteBuffer = {
    val len = 3 + (8 + 1 + edge._1.bytes.length + 4)
    val result = ByteBuffer.allocate(len)
    result.put(iter)
    result.putShort(1)
    result.putLong(edge._2.ts)
    result.put(edge._1.bytes.length.toByte)
    result.put(edge._1.bytes)
    result.put(edge._2.bytes)
    result.flip
    result
  }

  def decodePayload(payload: ByteBuffer): (Byte, java.util.Map[Vid, Edge]) = {
    if (payload == null) {
      null
    } else {
      val p = payload.position
      val iter = payload.get
      val size = payload.getShort.toInt
      var i = 1
      val result = new java.util.HashMap[Vid, Edge]
      while (i <= size) {
        val ts = payload.getLong
        val vidBytes = new Array[Byte](payload.get())
        payload.get(vidBytes)
        val vid = Vid(vidBytes)
        val edgeBytes = new Array[Byte](4)
        payload.get(edgeBytes)
        val edge = Edge.applyVersion(edgeBytes, ts)
        result.put(vid, edge)
        i += 1
      }
      payload.position(p)
      (iter, result)
    }
  }

}
