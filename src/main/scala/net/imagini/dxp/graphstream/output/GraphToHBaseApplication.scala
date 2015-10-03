package net.imagini.dxp.graphstream.output

import java.util.Properties

import org.apache.donut.DonutApp

/**
 * Created by mharis on 28/09/15.
 *
 * This component consumes the total output of bsp connected components using `graphdelta` topic and
 * off-loads the changes into HBase graph table.
 *
 */

class GraphToHBaseApplication(config: Properties) extends DonutApp[GraphToHBaseProcessingUnit]({

  val directMemMb = 512
  val heapMemMb = 1024
  /**
   * Memory Footprint (32 partitions in one topic) = 32 x 1.5Gb = 48 Gb
   */
  config.setProperty("group.id", "GraphstreamHBaseLoader")
  config.setProperty("task.memory.mb", "1024")
  config.setProperty("cogroup", "false")
  config.setProperty("topics", "graphdelta")
  config.setProperty("yarn1.restart.enabled", "true")
  config.setProperty("task.memory.mb", s"${directMemMb + heapMemMb}")
  config.setProperty("state.memory.mb", s"${directMemMb}")
  config.setProperty("yarn1.jvm.args", s"-XX:MaxDirectMemorySize=${directMemMb}m -Xmx${heapMemMb}m -Xms${heapMemMb}m -XX:NewRatio=3 -XX:+UseG1GC -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
  config.setProperty("hbase.table", "dxp-graph-v6")

  config
})
