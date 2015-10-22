package net.imagini.dxp.graphstream

import io.amient.donut.memstore.MemStoreClient
import net.imagini.dxp.common.BSPMessage
import net.imagini.dxp.graphstream.connectedbsp.ConnectedGraphBSPStreaming
import net.imagini.dxp.graphstream.debugging.{DebugConnectedBSP, DebugConnectedBSPApplication, GraphStatePrinter}
import net.imagini.dxp.graphstream.ingest.{SyncsToGraphStreaming}
import net.imagini.dxp.graphstream.output.{GraphToHBaseStreaming}


object YARNLaunchConnectedBSP extends App {
  ConnectedGraphBSPStreaming.main(Array(Config.path, "wait"))
}

object YARNLaunchSyncsToGraph extends App {
  SyncsToGraphStreaming.main(Array(Config.path, "wait"))
}

object YARNLaunchGraphToHBase extends App {
  GraphToHBaseStreaming.main(Array(Config.path, "wait"))
}

/**
 * Debugger launchers
 */

object DebugLocalSyncsToGraph extends App {
  new SyncsToGraphStreaming(Config).runLocally()
}

object DebugLocalConnectedBSP extends App {
  new DebugConnectedBSPApplication(Config).runLocally(debugOnePartition = 1)
}

object DebugMemStoreServerClient extends App {
  val client = new MemStoreClient("localhost", 50531)
  try {
    var count = 0L
    var unknownIdSpace = 0L
    client.foreach { case (key, value) => {
      try {
        val vid = BSPMessage.decodeKey(key)
        if (value.remaining > 0) {
          val payload = BSPMessage.decodePayload(value)
          if (payload._2.size > 29) {
            println(vid + " -> " + payload._2.size)
          }
          count += 1
        }
      } catch {
        case e: NoSuchElementException => {
          println(e.getMessage)
          unknownIdSpace += 1
        }
      }
    }
    }
    println(s"READ RECORDS COUNT = ${count}\nUNKNOWN ID SPACE COUNT = ${unknownIdSpace}")
  } finally {
    client.close
  }
}

object DebugYARNConnectedBSP extends App {
  DebugConnectedBSP.main(Array(Config.path, "wait"))
}

object DebugLocalGraphToHBase extends App {
  new GraphToHBaseStreaming(Config).runLocally(debugOnePartition = 0)
}

object DebugGraphDeltaPrinter extends App {
  GraphStatePrinter.main(Array(Config.path, "2"))
}
