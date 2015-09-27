package net.imagini.graphstream.syncstransform

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
  config.setProperty("yarn1.keepContainers", "true")
  config.setProperty("kafka.group.id", "GraphSyncsStreamingBSP")
  config.setProperty("kafka.topics", "datasync")
  config.setProperty("kafka.cogroup", "false")
  config
})

