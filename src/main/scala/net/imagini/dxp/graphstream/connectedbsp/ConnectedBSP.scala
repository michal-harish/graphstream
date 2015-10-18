package net.imagini.dxp.graphstream.connectedbsp

import java.io.FileInputStream
import java.util.Properties

/**
 * Created by mharis on 22/09/15.
 *
 * This class is for submitting the job with `./submit` script, e.g.:
 *
 * ./submit net.imagini.dxp.graphstream.connectedbsp.ConnectedBSP /etc/vdna/graphstream/config.properties
 */

object ConnectedBSP {
  def main(args: Array[String]) = {
    val config = new Properties
    config.load(new FileInputStream(args(0)))
    new ConnectedGraphBSPStreaming(config).runOnYarn(awaitCompletion = args.length == 2 && args(1) == "wait")
  }
}
