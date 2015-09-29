package net.imagini.dxp.graphstream.output

import java.util.Properties

import org.apache.donut.DonutApp

/**
 * Created by mharis on 28/09/15.
 */
class GraphToHBaseApplication(config: Properties) extends DonutApp[GraphToHBaseProcessingUnit]({
  config.setProperty("yarn1.keepContainers", "true")
  config.setProperty("kafka.group.id", "GraphstreamHBaseLoader")
  config.setProperty("kafka.topics", "graphdelta")
  config.setProperty("kafka.cogroup", "false")
  config.setProperty("hbase.table", "dxp-graph-v6")

  config
})
