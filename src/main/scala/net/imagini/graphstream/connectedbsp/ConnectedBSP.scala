package net.imagini.graphstream.connectedbsp

import java.io.FileInputStream
import java.util.Properties

/**
 * Created by mharis on 22/09/15.
 *
 * This class is for submitting the job with `./submit` script, e.g.:
 *
 * ./submit net.imagini.graphstream.connectedbsp.ConnectedBSP /etc/vdna/graphstream/config.properties
 */
object ConnectedBSP extends App {
  val config = new Properties
  config.load( new FileInputStream(args(0)))
  new ConnectedBSPApplication(config).runOnYarn(taskMemoryMb = 16 * 1024, awaitCompletion = false)
}
