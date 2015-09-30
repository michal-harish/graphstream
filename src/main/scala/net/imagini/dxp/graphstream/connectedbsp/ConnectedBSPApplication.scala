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
 * MEMORY FOOTPRINT
 * =========================================
 *  2 Gb - heap for processing per task
 *  8 Gb - off-heap for local state per task
 * =========================================
 * 10 Gb x 32 = 320 Gb
 */

class ConnectedBSPApplication(config: Properties) extends DonutApp[ConnectedBSPProcessingUnit]({
  config.setProperty("donut.task.memory.mb", "10240")
  config.setProperty("yarn1.keepContainers", "false")
  config.setProperty("yarn1.jvm.args", "-Xmx2g -Xms1g -XX:NewRatio=2 -XX:+UseG1GC -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
  config.setProperty("kafka.group.id", "GraphStreamingBSP")
  config.setProperty("kafka.topics", "graphdelta,graphstate")
  config.setProperty("kafka.cogroup", "true")
  config
})