package net.imagini.dxp.graphstream

import java.util.UUID

import net.imagini.dxp.common.{BSPMessage, Edge, Vid}
import net.jpountz.lz4.LZ4Factory
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by mharis on 02/10/15.
  */
class LZ4Test extends FlatSpec with Matchers {


   val compressor = LZ4Factory.fastestInstance.highCompressor
   val decompressor= LZ4Factory.fastestInstance.fastDecompressor

   val record = (1 to 280).map(i => Vid("vdna", UUID.randomUUID.toString) -> Edge("AAT", 1.0, 1L)).toMap

   val message = BSPMessage.encodePayload((1.toByte, record))

   println(s"raw message: ${message.length}")

   val compressedMessage = compressor.compress(message)

   println(s"compressed message: ${compressedMessage.length}")

   println(s"compression rate: ${compressedMessage.length.toDouble / message.length * 100} % ")
 }
