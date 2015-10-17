package net.imagini.dxp.graphstream

import net.imagini.dxp.graphstream.connectedbsp.ConnectedBSP
import net.imagini.dxp.graphstream.debugging.{DebugConnectedBSP, DebugConnectedBSPApplication, GraphStatePrinter}
import net.imagini.dxp.graphstream.ingest.{SyncsToGraph, SyncsToGraphApplication}
import net.imagini.dxp.graphstream.output.{GraphToHBase, GraphToHBaseApplication}
import org.apache.donut.KafkaUtils



object YARNLaunchConnectedBSP extends App {
  ConnectedBSP.main(Array(Config.path, "wait"))
}

object YARNLaunchSyncsToGraph extends App {
  SyncsToGraph.main(Array(Config.path, "wait"))
}

object YARNLaunchGraphToHBase extends App {
  GraphToHBase.main(Array(Config.path, "wait"))
}

/**
 * Debugger launchers
 */

object DebugLocalSyncsToGraph extends App {
  new SyncsToGraphApplication(Config).runLocally(debugOnePartition = 0)
}

object DebugLocalConnectedBSP extends App {
  new DebugConnectedBSPApplication(Config).runLocally(debugOnePartition = 6) //
}

object DebugYARNConnectedBSP extends App {
  DebugConnectedBSP.main(Array(Config.path, "wait"))
}

object DebugLocalGraphToHBase extends App {
  new GraphToHBaseApplication(Config).runLocally(debugOnePartition = 0)
}

object DebugGraphDeltaPrinter extends App {
  GraphStatePrinter.main(Array(Config.path, "2"))
}

object DebugOffsetReport extends App {

  val kafkaUtils = new KafkaUtils(Config)

  val inspect = Map(
    ("datasync" -> "GraphSyncsStreamingBSP"),
    ("graphdelta" ->  "GraphStreamingBSP"),
    ("graphstate" -> "GraphStreamingBSP")
  )
  kafkaUtils.getPartitionMap(inspect.keys.toList).foreach { case (topic, numPartitions) => {
    val consumerGroupId = inspect(topic)
      for (p <- (0 to numPartitions - 1)) {
        val consumer = new kafkaUtils.PartitionConsumer(topic, p, consumerGroupId)

        val (earliest, consumed, latest) = (consumer.getEarliestOffset, consumer.getOffset, consumer.getLatestOffset)

        println(s"$topic/$p OFFSET RANGE = ${earliest}:${latest} => ${consumerGroupId} group offset ${consumed} }")
      }
    }
  }
}
