package net.imagini.dxp.graphstream.debugging

import java.io.FileInputStream
import java.util.Properties
import io.amient.donut.KafkaUtils
import net.imagini.dxp.common.BSPMessage

/**
 * Created by mharis on 22/09/15.
 */

object GraphStatePrinter {
  def main(args: Array[String]) : Unit = {
    val config = new Properties
    config.load( new FileInputStream(args(0)))
    val minEdges = args(1).toInt
    val kafkaUtils = new KafkaUtils(config)
    kafkaUtils.createDebugConsumer("graphstate", (key, msg) => {
      val vid = BSPMessage.decodeKey(key)
      val payload = msg match {
        case null => null
        case x => BSPMessage.decodePayload(x)
      }
      if (payload != null && payload._2.size >= minEdges) {
        println(s"${vid} -> ${payload}")
      }
    })
  }
}
