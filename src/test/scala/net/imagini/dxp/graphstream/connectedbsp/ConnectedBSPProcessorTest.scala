package net.imagini.dxp.graphstream.connectedbsp

import java.nio.ByteBuffer
import kafka.producer.KeyedMessage
import net.imagini.dxp.common.{Vid, Edge, BSPMessage}
import org.apache.donut.utils.ByteUtils
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by mharis on 26/09/15.
 */
class ConnectedBSPProcessorTest extends FlatSpec with Matchers {

  behavior of "ConnectedBSPProcessor"

  val processor = new ConnectedBSPProcessor(maxStateSizeMb = 1024, minEdgeProbability = 0.39)

  def aatEdge(p: Double, ts: Long) = Edge("AAT", p, ts)

  val r1 = Vid("r", "1")
  val a1 = Vid("a", "1")
  val a2 = Vid("a", "2")
  val r2 = Vid("r", "2")


  it should "generate minimum messages and base state for new sync pair (r->a1),(a1->r)" in {
    val inputMap1 = Map(a1 -> aatEdge(0.9, 1))
    val input1 = encode(r1, (1.toByte, inputMap1))
    val output1 = processRecursively(input1._1, input1._2)
    output1.size should be(1)
    output1(0) should be(message("graphstate", input1))
    print(output1)
    val inputMap2 = Map(r1 -> aatEdge(0.9, 1))
    val input2 = encode(a1, (1.toByte, inputMap2))
    val output2 = processRecursively(input2._1, input2._2)
    output2.size should be(1)
    print(output2)
    val state = getState
    print(state)
    processor.state.size should be(2)
    processor.state.contains(ByteBuffer.wrap(r1.bytes)) should be(true)
    processor.state.contains(ByteBuffer.wrap(a1.bytes)) should be(true)
    println(BSPMessage.decodePayload(processor.state.get(ByteBuffer.wrap(r1.bytes)).get))
    println(BSPMessage.decodePayload(input1._2))
    processor.state.get(ByteBuffer.wrap(r1.bytes)).get.equals(input1._2) should be(true)
    processor.state.get(ByteBuffer.wrap(a1.bytes)).get.equals(input2._2) should be(true)
    state.size should be(2)
    state.get(r1).get should be(inputMap1)
    state.get(a1).get should be(inputMap2)
  }

  it should "yield intermediate state for first sync pair (r->a2)" in {
    val input = encode(r1, (1.toByte, Map(a2 -> aatEdge(0.5, 2))))
    val output = processRecursively(input._1, input._2)
    output.size should be(8)
    print(output)
    val state = getState
    print(state)
    output(2) should be(message("graphstate",
      (BSPMessage.encodeKey(r1), BSPMessage.encodePayload((1.toByte, Map(a1 -> aatEdge(0.9, 1), a2 -> aatEdge(0.5, 2)))))))
    output(5) should be(message("graphstate",
      (BSPMessage.encodeKey(a1), BSPMessage.encodePayload((2.toByte, Map(r1 -> aatEdge(0.9, 1), a2 -> aatEdge(0.45, 2)))))))
    output(6) should be(message("graphstate",
      (BSPMessage.encodeKey(a2), BSPMessage.encodePayload((3.toByte, Map(r1 -> aatEdge(0.4, 2))))))) // TODO verify why 0.4
    output(7) should be(message("graphstate",
      (BSPMessage.encodeKey(a2), BSPMessage.encodePayload((2.toByte, Map(r1 -> aatEdge(0.4, 2), a1 -> aatEdge(0.45, 2)))))))
    state.size should be(3)
  }

  it should "yield final correct state for the second sync pair (a2->r)" in {
    val input = encode(a2, (1.toByte, Map(r1 -> aatEdge(0.5, 2))))
    val output = processRecursively(input._1, input._2)
    output.size should be(1)
    print(output)
    val state = getState
    print(state)
    output(0) should be(message("graphstate",
      (BSPMessage.encodeKey(a2), BSPMessage.encodePayload((1.toByte, Map(r1 -> aatEdge(0.5, 2), a1 -> aatEdge(0.45, 2)))))))
    state.size should be(3)
    state.get(a2).get should be(Map(r1 -> aatEdge(0.5, 2), a1 -> aatEdge(0.45, 2)))
    state.get(a1).get should be(Map(r1 -> aatEdge(0.9, 1), a2 -> aatEdge(0.45, 2)))
    state.get(r1).get should be(Map(a1 -> aatEdge(0.9, 1), a2 -> aatEdge(0.5, 2)))
  }
  it should "yield final correct state for another sync (r2->a2, a2->r2)" in {
    val input1 = encode(a2, (1.toByte, Map(r2 -> aatEdge(0.8, 3))))
    val output1 = processRecursively(input1._1, input1._2)
    val input2 = encode(r2, (1.toByte, Map(a2 -> aatEdge(0.8, 3))))
    val output2 = processRecursively(input2._1, input2._2)
    output1.size should be(9)
    print(output1)
    val state = getState
    print(state)
//    output1(4) should be(message("graphstate",
//      (BSPMessage.encodeKey(a2), BSPMessage.encodePayload((1.toByte, Map(r1 -> aatEdge(0.5, 2), a1 -> aatEdge(0.45, 2)))))))
    state.size should be(4)
    state.get(r2).get should be(Map(r1 -> aatEdge(0.396078431372549, 3), a2 -> aatEdge(0.8, 3)))
    state.get(a2).get should be(Map(r1 -> aatEdge(0.5, 2), a1 -> aatEdge(0.45, 2), r2 -> aatEdge(0.8, 3)))
    state.get(a1).get should be(Map(r1 -> aatEdge(0.9, 1), a2 -> aatEdge(0.45, 2)))
    state.get(r1).get should be(Map(a1 -> aatEdge(0.9, 1), a2 -> aatEdge(0.5, 2), r2 -> aatEdge(0.396078431372549, 3)))
  }

  //TODO test another layer of connection added on top of the ones above

  //TODO test larger volume behaviour with eviction

  private def processRecursively(key: ByteBuffer, payload: ByteBuffer): List[KeyedMessage[ByteBuffer, ByteBuffer]] = {
    val output = processor.processDeltaInput(key, payload)
    output ++ output.filter(_.topic == "graphdelta").flatMap(message => {
      processRecursively(message.key, message.message)
    })
  }

  private def message(topic: String, encoded: (ByteBuffer, ByteBuffer)) = {
    new KeyedMessage(topic, encoded._1, encoded._2)
  }

  private def encode(key: Vid, payload: (Byte, Map[Vid, Edge]))
  = (BSPMessage.encodeKey(key), BSPMessage.encodePayload(payload))

  private def getState: Map[Vid, Map[Vid, Edge]] = {
    processor.state.iterator.map { case (keyBytes, valBytes) =>
      BSPMessage.decodeKey(keyBytes) -> BSPMessage.decodePayload(valBytes)._2
    }.toMap
  }

  private def print(messages: List[KeyedMessage[ByteBuffer, ByteBuffer]]): Unit = {
    messages.foreach(message => {
      println(s"${message.topic}: ${BSPMessage.decodeKey(message.key)} -> ${BSPMessage.decodePayload(message.message)}")
    })
  }

  private def print(state: Map[Vid, Map[Vid, Edge]]) = {
    println(s"== STATE ${processor.state.size} =======================================================================")
    getState.foreach(x => println(s"${x._1} -> ${x._2}"))
    println("=========================================================================================================")
  }

}
