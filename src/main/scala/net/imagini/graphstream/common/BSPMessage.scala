package net.imagini.graphstream.common

import java.io.DataInput
import java.nio.ByteBuffer

import com.google.common.io.ByteStreams

/**
 * Created by mharis on 10/09/15.
 */
object BSPMessage {


  def encodeKey(key: Vid): Array[Byte] = key.bytes

  def encodePayload(payload: (Byte, Map[Vid, Edge])): Array[Byte] = {
    val (iter, edges) = payload
    val len = edges.foldLeft(1 + 2)((l, item) => l + 8 + 1 + item._1.bytes.length + 4)
    val result = ByteBuffer.allocate(len)
    result.put(iter)
    result.putShort(edges.size.toShort)
    edges.foreach { case (k, v) => {
      result.putLong(v.ts)
      result.put(k.bytes.length.toByte)
      result.put(k.bytes)
      result.put(v.bytes)
    }
    }
    result.array
  }

  def decodeKey(key: Array[Byte]): Vid = Vid(key)

  //TODO declare dependency on guava ByteStreams

  def decode(bytes: Array[Byte]): (Byte, Map[Vid, Edge]) = if (bytes == null) null else decode(ByteStreams.newDataInput(bytes, 0))

  def decode(buffer: ByteBuffer): (Byte, Map[Vid, Edge]) = if (buffer == null) null else decode(ByteStreams.newDataInput(buffer.array, buffer.arrayOffset))

  def decode(input: DataInput): (Byte, Map[Vid, Edge]) = {
    val iter = input.readByte
    val size = input.readShort.toInt
    (iter, (for (i <- (1 to size)) yield {
      val ts = input.readLong
      val vidBytes = new Array[Byte](input.readByte)
      input.readFully(vidBytes)
      val vid = Vid(vidBytes)
      val edgeBytes = new Array[Byte](4)
      input.readFully(edgeBytes)
      val edge = Edge.applyVersion(edgeBytes, ts)
      (vid, edge)
    }).toMap)
  }

}

