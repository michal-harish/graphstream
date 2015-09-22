package net.imagini.graphstream.connectedbsp

import java.io.FileInputStream
import java.util.Properties

import net.imagini.graphstream.syncstransform.SyncsToGraph._
import org.apache.donut.DonutApp

/**
 * Created by mharis on 14/09/15.
 *
 * This is a stateful recursive streaming processor. Each unit (ConnectedBSPProcessUnit) processes cogrouped partitions
 * from 2 topics, one for Delta and one for State:
 *
 * A. the Delta is recursively processed from and to topic 'graphstream'
 * B. the State is kept in a compacted topic 'graphstate'
 *
 * The input into this application comes from SyncsTransformApplication which provides fresh edges into the graph.
 * The input is amplified by recursive consulation of State and production of secondary delta messages.
 */

object ConnectedBSP extends App {
  val config = new Properties
  config.load( new FileInputStream(args(0)))
  new ConnectedBSP(config).runOnYarn(taskMemoryMb = 20 * 1024, awaitCompletion = false)
}

class ConnectedBSP(config: Properties) extends DonutApp[ConnectedBSPProcessUnit]({
  /**
   *  GraphStreamingBSP component configuration
   */
  config.setProperty("yarn1.keepContainers", "true")
  config.setProperty("kafka.group.id", "GraphStreamingBSP")
  config.setProperty("kafka.topics", "graphstream,graphstate")
  config.setProperty("kafka.cogroup", "true")
  config
})