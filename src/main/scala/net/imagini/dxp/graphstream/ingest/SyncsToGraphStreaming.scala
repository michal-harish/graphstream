package net.imagini.dxp.graphstream.ingest

import java.io.FileInputStream
import java.util.Properties

import io.amient.donut.DonutApp


/**
 * Created by mharis on 22/09/15.
 *
 * This class is for submitting the job with `./submit` script, e.g.:
 *
 * ./submit net.imagini.dxp.graphstream.ingest.SyncsToGraphStreaming /etc/vdna/graphstream/config.properties
 */

object SyncsToGraphStreaming {
  def main(args: Array[String]) = {
    val config = new Properties
    config.load( new FileInputStream(args(0)))
    new SyncsToGraphStreaming(config).runOnYarn(awaitCompletion = args.length == 2 && args(1) == "wait")
  }
}

/**
 * Created by mharis on 15/09/15.
 *
 * This is a simple transformation streaming processor. Each unit (SyncsToGraphProcessUnit) processes fixed
 * set of partitions from json serialized 'datasync' topic and transforms each sync (a pair of connected IDs)
 * to a pair of messages representing a delta edge and reverse edge between the IDs into 'graphstream' topic.
 *
 */

class SyncsToGraphStreaming(config: Properties) extends DonutApp[SyncsToGraphProcessingUnit]({

  //Memory Footprint: 6 x 1Gb = 6 Gb
  config.setProperty("group.id", "GraphSyncsStreamingBSP")
  config.setProperty("topics", "datasync")
  config.setProperty("cogroup", "false")
  config.setProperty("max.tasks", "6")
  config.setProperty("direct.memory.mb", "0") // 0 - no local state for simple stream-to-stream transformation
  config.setProperty("task.overhead.memory.mb", "1024")
  config.setProperty("yarn1.jvm.args", "-XX:+UseSerialGC -XX:NewRatio=5 -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
  config.setProperty("yarn1.restart.enabled", "true")
  config
})

