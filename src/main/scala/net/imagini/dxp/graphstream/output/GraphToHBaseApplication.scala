package net.imagini.dxp.graphstream.output

import java.util.Properties

import org.apache.donut.DonutApp

/**
 * Created by mharis on 28/09/15.
 *
 * This component consumes the total output of bsp connected components using `graphdelta` topic and
 * off-loads the changes into HBase graph table.
 */

class GraphToHBaseApplication(config: Properties) extends DonutApp[GraphToHBaseProcessingUnit]({
  config.setProperty("donut.task.memory.mb", "1024")
  config.setProperty("yarn1.keepContainers", "true")
  config.setProperty("yarn1.jvm.args", "-Xmx768m -Xms512m -XX:NewRatio=4 -XX:+UseG1GC -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
  config.setProperty("kafka.group.id", "GraphstreamHBaseLoader")
  config.setProperty("kafka.topics", "graphdelta")
  config.setProperty("kafka.cogroup", "false")
  config.setProperty("hbase.table", "dxp-graph-v6")

  config
})
