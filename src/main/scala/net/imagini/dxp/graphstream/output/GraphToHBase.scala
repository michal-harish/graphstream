package net.imagini.dxp.graphstream.output

import java.io.FileInputStream
import java.util.Properties

/**
 * Created by mharis on 28/09/15.
 *
 * This class is for submitting the job with `./submit` script, e.g.:
 *
 * ./submit net.imagini.dxp.graphstream.output.GraphToHBase /etc/vdna/graphstream/config.properties
 *
 */

object GraphToHBase {
  def main(args: Array[String]) = {
    val config = new Properties
    config.load( new FileInputStream(args(0)))
    new GraphToHBaseApplication(config).runOnYarn(awaitCompletion = args.length == 2 && args(1) == "wait")
  }
}
