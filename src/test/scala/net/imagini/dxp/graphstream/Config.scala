package net.imagini.dxp.graphstream

import java.io.FileInputStream
import java.util.Properties

/**
 * Created by mharis on 01/10/15.
 */
object ConfigMain extends Properties {
  val path = "/etc/vdna/graphstream/main.properties"
  load( new FileInputStream(path))
}

object ConfigBridge extends Properties {
  val path = "/etc/vdna/graphstream/bridge.properties"
  load( new FileInputStream(path))
}
