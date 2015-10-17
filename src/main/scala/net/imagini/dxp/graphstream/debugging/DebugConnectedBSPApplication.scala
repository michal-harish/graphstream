package net.imagini.dxp.graphstream.debugging

import java.util.Properties

import net.imagini.dxp.graphstream.connectedbsp.ConnectedBSPProcessingUnit
import org.apache.donut._

/**
 * Created by mharis on 08/10/15.
 */

class DebugConnectedBSPApplication(config: Properties) extends DonutApp[ConnectedBSPProcessingUnit]({

  config.setProperty("debug", "true")
  config.setProperty("group.id", "DebugGraphStateBuilder")
  config.setProperty("topics", "graphstate,graphdelta")
  config.setProperty("cogroup", "true");
  config.setProperty("direct.memory.mb", "32000")
  config.setProperty("task.overhead.memory.mb", "2048")
  config.setProperty("yarn1.jvm.args", "-XX:+UseSerialGC -XX:NewRatio=3 -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
  config.setProperty("yarn1.restart.enabled", "false")
  config
})

