package net.imagini.dxp.graphstream.debugging

import java.util.Properties

import org.apache.donut._

/**
 * Created by mharis on 08/10/15.
 */

class GraphStateCompactorApp(config: Properties) extends DonutApp[GraphStateCompactorProcessor]({

  config.setProperty("group.id", "DebugGraphStateBuilder")
  config.setProperty("topics", "graphstate")
  config.setProperty("direct.memory.mb", "200000")
  config.setProperty("task.overhead.memory.mb", "1024")
  config.setProperty("yarn1.jvm.args", "-XX:+UseSerialGC -XX:NewRatio=3 -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
  config.setProperty("yarn1.restart.enabled", "false")
  config.setProperty("yarn1.restart.failed.retries", "3")
  config
})

