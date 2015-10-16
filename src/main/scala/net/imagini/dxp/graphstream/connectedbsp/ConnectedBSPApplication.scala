package net.imagini.dxp.graphstream.connectedbsp

import java.util.Properties
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
 *
 */

class ConnectedBSPApplication(config: Properties) extends DonutApp[ConnectedBSPProcessingUnit]({

  // Memory Footprint (32 partitions in both topics) = 200g + (32 x 1g overhead) + 3g= 335 Gb
  config.setProperty("group.id", "GraphStreamingBSP")
  config.setProperty("topics", "graphdelta,graphstate")
  config.setProperty("cogroup", "true")
  config.setProperty("direct.memory.mb",      "200000")
  config.setProperty("task.overhead.memory.mb", "1024")
  config.setProperty("yarn1.jvm.args", "-XX:+UseSerialGC -XX:NewRatio=2 -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
  config.setProperty("yarn1.restart.enabled", "true")
  config.setProperty("yarn1.restart.failed.retries", "3")
  config.setProperty("yarn1.master.memory.mb", "3072")
  config
})