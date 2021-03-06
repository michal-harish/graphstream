package net.imagini.dxp.graphstream.output

import java.io.FileInputStream
import java.util.Properties

import io.amient.donut.DonutApp


/**
 * Created by mharis on 28/09/15.
 *
 * This class is for submitting the job with `./submit` script, e.g.:
 *
 * ./submit net.imagini.dxp.graphstream.output.GraphToHBaseStreaming /etc/vdna/graphstream/main.properties
 *
 */

object GraphToHBaseStreaming {
  def main(args: Array[String]) = {
    val config = new Properties
    config.load( new FileInputStream(args(0)))
    new GraphToHBaseStreaming(config).runOnYarn(awaitCompletion = args.length == 2 && args(1) == "wait")
  }
}

/**
 * Created by mharis on 28/09/15.
 *
 * This component consumes the total output of bsp connected components using `graphdelta` topic and
 * off-loads the changes into HBase graph table.
 *
 */

class GraphToHBaseStreaming(config: Properties) extends DonutApp[GraphToHBaseProcessingUnit]({

  //Memory Footprint (32 partitions in one topic) = (32 x 1.5Gb) = 48 Gb
  config.setProperty("group.id", "GraphstreamHBaseLoader")
  config.setProperty("task.memory.mb", "5000")
  config.setProperty("cogroup", "false")
  config.setProperty("topics", "graphdelta")
  config.setProperty("direct.memory.mb", "0") // 0 - no local state for simple stream-to-stream transformation
  config.setProperty("task.overhead.memory.mb", "1536") //1.5g - microbatching multi-puts to hbase generates a lot of objects in the process
  config.setProperty("yarn1.jvm.args", "-XX:+UseSerialGC -XX:NewRatio=3 -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
  config.setProperty("yarn1.restart.enabled", "true")
  config.setProperty("hbase.table", "dxp-graph-v6")

  config
})
