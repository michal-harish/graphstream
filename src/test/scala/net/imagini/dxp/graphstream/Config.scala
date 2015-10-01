package net.imagini.dxp.graphstream

import java.io.FileInputStream
import java.util.Properties

/**
 * Created by mharis on 01/10/15.
 */
object Config extends Properties {
  val path = "/etc/vdna/graphstream/config.properties"
  load( new FileInputStream(path))
}
