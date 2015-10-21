package net.imagini.dxp.graphstream

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
  SyncsToGraphStreaming.main(Array(Config.path, "wait"))
}

/**
 * Debugger launchers
 */

object DebugLocalSyncsToGraph extends App {
  new SyncsToGraphStreaming(Config).runLocally()
}

object DebugLocalConnectedBSP extends App {
  new DebugConnectedBSPApplication(Config).runLocally() //(debugOnePartition = 1)
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
