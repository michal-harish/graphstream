package net.imagini.dxp.graphstream

import net.imagini.dxp.graphstream.connectedbsp.{ConnectedBSP, ConnectedBSPApplication}
import net.imagini.dxp.graphstream.debugging.{GraphStateBuilder, GraphDeltaPrinter, GraphStatePrinter}
import net.imagini.dxp.graphstream.ingest.{SyncsToGraph, SyncsToGraphApplication}
import net.imagini.dxp.graphstream.output.{GraphToHBase, GraphToHBaseApplication}
import org.apache.donut.KafkaUtils

/**
 * Created by mharis on 14/09/15.
 *
 * GraphStream components for launching from an IDE
 */

object ConnectedBSPLocalLauncher extends App {
  new ConnectedBSPApplication(Config).runLocally(testOnlyOnePartition = true)
}

object ConnectedBSPYarnLauncher extends App {
  ConnectedBSP.main(Array(Config.path, "wait"))
}

object SyncsToGraphLocalLauncher extends App {
  new SyncsToGraphApplication(Config).runLocally(testOnlyOnePartition = true)
}

object SyncsToGraphYarnLauncher extends App {
  SyncsToGraph.main(Array(Config.path, "wait"))
}

object GraphToHBaseLocalLauncher extends App {
  new GraphToHBaseApplication(Config).runLocally(testOnlyOnePartition = true)
}

object GraphToHBaseYarnLauncher extends App {
  GraphToHBase.main(Array(Config.path, "wait"))
}

/**
 * Debugger launchers
 */

object DebugGraphStateBuilder extends App {
  new GraphStateBuilder(Config).runLocally(testOnlyOnePartition = true)
}

object DebugGraphStream extends App {
  GraphStatePrinter.main(Array(Config.path, "2"))
}

object DebugGraphState extends App {
  GraphDeltaPrinter.main(Array(Config.path, "2"))
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
