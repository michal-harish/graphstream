package net.imagini.graphstream.syncstransform

import java.io.FileInputStream
import java.util.Properties
import org.apache.donut.DonutApp

/**
 * Created by mharis on 15/09/15.
 *
 * This is a simple transformation streaming processor. Each unit (SyncsToGraphProcessUnit) processes fixed
 * set of partitions from json serialized 'datasync' topic and transforms each sync (a pair of connected IDs)
 * to a pair of messages representing a delta edge and reverse edge between the IDs into 'graphstream' topic.
 */
object SyncsToGraph extends App {
  val config = new Properties
  config.load( new FileInputStream(args(0)))
  new SyncsToGraph(config).runOnYarn(taskMemoryMb = 4 * 1024, awaitCompletion = false)
}

class SyncsToGraph(config: Properties) extends DonutApp[SyncsToGraphProcessUnit]({
  /**
   *  GraphSyncsStreamingTransform component configuration
   */
  config.setProperty("yarn1.keepContainers", "true")
  config.setProperty("kafka.group.id", "GraphSyncsStreamingBSP")
  config.setProperty("kafka.topics", "datasync")
  config.setProperty("kafka.cogroup", "false")
  config
})

