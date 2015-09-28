package net.imagini.dxp.common

import org.scalatest._

class VidTest extends FlatSpec with Matchers {

  //HEX Vids
  val aid = Vid("aidsha1", "624d19d7f542b3b98d59fff556c68f63fad28c10")
  val s = aid.toString
  println(s)
  aid.toString should be("624d19d7f542b3b98d59fff556c68f63fad28c10:aidsha1")


  //String Vids
  val d = Vid("d", "2")
  val r = Vid("r", "1")
  Seq(d, r).sortWith(Vid.comparator) should be (Seq(r, d))

  val d0 = Vid("d", "CESE1111")
  d0.bytes.mkString(",") should be("3,-126,76,116,0,100,67,69,83,69,49,49,49,49")
  d0.toString should be ("CESE1111:d")
  d0.asString should be ("CESE1111")
  val d1 = Vid("d", "CESE9999")
  d1.bytes.mkString(",") should be("3,-122,14,116,0,100,67,69,83,69,57,57,57,57")
  d1.toString should be ("CESE9999:d")
  d1.asString should be ("CESE9999")

  //UUID Vids
  val v0 = Vid("vdna", "f81d4fae-7dec-11d0-a765-00a0c91e6bf6")
  v0.bytes.mkString(",") should be("-8,29,79,-82,40,-31,-8,29,79,-82,125,-20,17,-48,-89,101,0,-96,-55,30,107,-10")
  v0.toString should be ("f81d4fae-7dec-11d0-a765-00a0c91e6bf6:vdna")
  v0.asString should be ("f81d4fae-7dec-11d0-a765-00a0c91e6bf6")

  val v1 = Vid("vdna", "f81d4fae-7dec-11d0-a765-00a0c91e6bf6")
  val v2 = Vid("vdna", "f81d4fae-7dec-11d0-a765-00a0c91e6bf7")
  val x3 = Vid("aaid", "f81d4fae-7dec-11d0-a765-00a0c91e6bf8")
  x3.toString should be ("f81d4fae-7dec-11d0-a765-00a0c91e6bf8:aaid")
  x3.asString should be ("f81d4fae-7dec-11d0-a765-00a0c91e6bf8")

  v0.compareTo(v1) should be(0)
  v0.compareTo(v2) should be < 0
  x3.compareTo(v0) should be > 0
  x3.compareTo(v2) should be > 0

  v0.equals(v1) should be(true)
  v0.hashCode should equal(v1.hashCode)
  v1.compareTo(v2) should be < 0
  v2.compareTo(v1) should be > 0
  x3.compareTo(v1) should be > 0
  v1.equals(v2) should be(false)
  v2.equals(v2) should be(true)
  v2.idSpace should be(IdSpace.vdna)
  v2.isVdna should be(true)
  x3.idSpace should not be (IdSpace.vdna)
  x3.isVdna should be(false)

  val seq = Seq(v0, v1, v2, x3)
  val customComparatorSortedSeq = seq.sortWith(Vid.comparator)
  val sortedSeq = seq.sorted

  customComparatorSortedSeq should be(sortedSeq)
}