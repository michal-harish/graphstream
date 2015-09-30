package net.imagini.dxp.graphstream.ingest

import java.util.Properties

import org.apache.donut.DonutApp

/**
 * Created by mharis on 15/09/15.
 *
 * This is a simple transformation streaming processor. Each unit (SyncsToGraphProcessUnit) processes fixed
 * set of partitions from json serialized 'datasync' topic and transforms each sync (a pair of connected IDs)
 * to a pair of messages representing a delta edge and reverse edge between the IDs into 'graphstream' topic.
 */

class SyncsToGraphApplication(config: Properties) extends DonutApp[SyncsToGraphProcessingUnit]({
  config.setProperty("donut.task.memory.mb", "1024")
  config.setProperty("yarn1.keepContainers", "true")
  config.setProperty("yarn1.jvm.args", "-Xmx768m -Xms512m -XX:NewRatio=5 -XX:+UseG1GC -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
  config.setProperty("kafka.group.id", "GraphSyncsStreamingBSP")
  config.setProperty("kafka.topics", "datasync")
  config.setProperty("kafka.cogroup", "false")
  config
})

