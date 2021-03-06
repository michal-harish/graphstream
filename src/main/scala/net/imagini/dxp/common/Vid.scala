
package net.imagini.dxp.common

import java.nio.ByteBuffer

import io.amient.utils.ByteUtils


class Vid(val isVdna: Boolean, val idSpace: Short, val bytes: Array[Byte], val hash: Int)
extends java.io.Serializable with Ordered[Vid] {
  override def compareTo(that: Vid): Int = {
    ByteUtils.compare(this.bytes, 0, this.bytes.length, that.bytes, 0, that.bytes.length)
  }

  override def compare(that: Vid): Int = compareTo(that)

  override def hashCode: Int = hash

  override def equals(other: Any) = other != null && other.isInstanceOf[Vid] && equals(other.asInstanceOf[Vid])

  def equals(that: Vid) = (this.hash == that.hash) && ByteUtils.equals(this.bytes, that.bytes)

  override def toString = IdSpace(idSpace).toString(bytes)

  def asString = IdSpace(idSpace).asString(bytes)
}

object Vid {

  def apply(idSpace: String, id: String): Vid = apply(IdSpace(idSpace), id)

  def apply(vdnaUserId: String): Vid = apply(IdSpace.vdna, vdnaUserId)

  def apply(idSpace: Short, id: String): Vid = apply(IdSpace(idSpace).asBytes(id))

  def apply(id: Array[Byte]): Vid = {
    val idSpace: Short = IdSpace(id, 0, id.length)
    val isVdna: Boolean = idSpace == IdSpace.vdna
    ByteUtils.asIntValue(id) match {
      case 0 | Int.MinValue => {
        ByteUtils.putIntValue(Int.MaxValue, id, 0)
        new Vid(isVdna, idSpace, id, Int.MaxValue)
      }
      case h => new Vid(isVdna, idSpace, id, h)
    }
  }

  def comparator(a: Vid, b: Vid) = a.compareTo(b) < 0

  def higher(a: Vid, b: Vid): Vid = if (a.compareTo(b) >= 0) a else b

  def higher[T](a: (Vid,T), b: (Vid,T)) : (Vid, T) = if (a._1.compareTo(b._1) >= 0) a else b

  def highest(a: Vid, bc: Iterable[Vid]): Vid = bc.foldLeft(a)((b, c) => if (b.compareTo(c) >= 0) b else c)

  def highest[T](a: (Vid,T), bc: Iterable[(Vid,T)]): (Vid, T) = if (bc.isEmpty) a else {
    bc.foldLeft(a)((b, c) => if (b._1.compareTo(c._1) >= 0) b else c)
  }

  def highest[T](bc: Iterable[(Vid,T)]): (Vid, T) = if (bc.isEmpty) null else {
    bc.reduce((b, c) => if (b._1.compareTo(c._1) >= 0) b else c)
  }

  private val maxHash = BigInt(1, Array(255.toByte,255.toByte,255.toByte,255.toByte))

  def getPartition(numPartitions: Int, vid: Vid):Int = getPartition(numPartitions, vid.bytes, 0)

  def getPartition(numPartitions: Int, vidArray: Array[Byte]):Int = getPartition(numPartitions, vidArray, 0)

  def getPartition(numPartitions: Int, vidArray: Array[Byte], vidArrayOffset: Int): Int = {
    getPartition(numPartitions, BigInt(1, vidArray.slice(vidArrayOffset, vidArrayOffset + 4)))
  }

  def getPartition(numPartitions: Int, b: ByteBuffer):Int = {
    val hashCode:BigInt = (b.get(b.position + 3) & 0xFF) +
      ((b.get(b.position + 2) & 0xFF) << 8) +
      ((b.get(b.position + 1) & 0xFF) << 16) + (BigInt(b.get(b.position) & 0xFF) << 24)
    getPartition(numPartitions, hashCode)
  }

  def getPartition(numPartitions: Int, hashCode: BigInt): Int = {
    val unit = maxHash / (numPartitions)
    val p = (hashCode / unit).toInt
    math.min(p, numPartitions-1)
  }

}





