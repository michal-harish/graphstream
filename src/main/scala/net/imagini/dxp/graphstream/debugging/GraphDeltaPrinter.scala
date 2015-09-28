package net.imagini.dxp.graphstream.debugging

import java.io.FileInputStream
import java.util.Properties
import net.imagini.dxp.common.BSPMessage
import org.apache.donut.KafkaUtils

/**
 * Created by mharis on 22/09/15.
 */
object GraphDeltaPrinter  {
  def main(args: Array[String]) : Unit = {
    val config = new Properties
    config.load( new FileInputStream(args(0)))
    val minEdges = args(1).toInt
    val kafkaUtils = new KafkaUtils(config)
    kafkaUtils.createDebugConsumer("graphdelta", (msg) => {
      val vid = BSPMessage.decodeKey(msg.key)
      val payload = msg.message match {
        case null => null
        case x => BSPMessage.decodePayload(x)
      }
      if (payload != null && payload._2.size >= minEdges) {
        println(s"${vid} -> ${payload}")
      }
    })
  }
}

