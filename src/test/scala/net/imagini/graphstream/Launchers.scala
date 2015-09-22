package net.imagini.graphstream

import java.io.FileInputStream
import java.util.Properties

import net.imagini.graphstream.common.BSPMessage
import net.imagini.graphstream.connectedbsp.ConnectedBSP
import net.imagini.graphstream.syncstransform.SyncsToGraph
import org.apache.donut.KafkaUtils

/**
 * Created by mharis on 14/09/15.
 */

object Config extends Properties {
  load( new FileInputStream("/etc/vdna/graphstream/config.properties"))
}

object ConnectedBSPLocalLauncher extends App {
  new ConnectedBSP(Config).runLocally(multiThreadMode = false)
}

object ConnectedBSPYarnLauncher extends App {
  new ConnectedBSP(Config).runOnYarn(taskMemoryMb = 20 * 1024, awaitCompletion = true)
}

object SyncsToGraphLocalLauncher extends App {
  new SyncsToGraph(Config).runLocally(multiThreadMode = false)
}

object SyncsToGraphYarnLauncher extends App {
  new SyncsToGraph(Config).runOnYarn(taskMemoryMb = 16 * 1024, awaitCompletion = true )
}

object DebugGraphStream extends App {
  val kafkaUtils = new KafkaUtils(Config)
  kafkaUtils.createDebugConsumer("graphstream", (msg) => {
    val vid = BSPMessage.decodeKey(msg.key)
    val payload = msg.message match {
      case null => null
      case x => BSPMessage.decodePayload(x)
    }
    if (payload != null && payload._2.size > 0) {
      println(s"${vid} -> ${payload}")
    }
  })
}

object DebugGraphState extends App {
  val kafkaUtils = new KafkaUtils(Config)
  kafkaUtils.createDebugConsumer("graphstate", (msg) => {
    val vid = BSPMessage.decodeKey(msg.key)
    val payload = msg.message match {
      case null => null
      case x => BSPMessage.decodePayload(x)
    }
    if (payload != null && payload._2.size > 1) {
      print(s"\n${vid} -> ${payload}")
    }
  })
}

object DebugOffsetReport extends App {

  val kafkaUtils = new KafkaUtils(Config)

  val inspect = Map(
    ("datasync" -> "GraphSyncsStreamingBSP"),
    ("graphstream" ->  "GraphStreamingBSP"),
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
