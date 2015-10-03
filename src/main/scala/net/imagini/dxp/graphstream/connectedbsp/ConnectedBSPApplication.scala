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

  val directMemMb = 6144
  val heapMemMb = 2048
  /**
   * Memory Footprint (32 partitions in both topics) = 32 x (6g + 2g) = 256 Gb
   */
  config.setProperty("group.id", "GraphStreamingBSP")
  config.setProperty("topics", "graphdelta,graphstate")
  config.setProperty("cogroup", "true")
  config.setProperty("yarn1.restart.enabled", "true")
  config.setProperty("yarn1.restart.failed.retries", "3")

  //TODO config.setProperty("task.direct.memory.mb", s"${directMemMb}")
  //TODO config.setProperty("task.heap.memory.mb", s"${heapMemMb}")
  config.setProperty("task.memory.mb", s"${directMemMb + heapMemMb}")
  config.setProperty("state.memory.mb", s"${directMemMb}")

  config.setProperty("yarn1.jvm.args", s"-XX:MaxDirectMemorySize=${directMemMb}m -Xmx${heapMemMb}m -Xms${heapMemMb}m -XX:NewRatio=2 -XX:+UseSerialGC -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
  config
})