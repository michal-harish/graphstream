package net.imagini.dxp.graphstream.ingest

import java.io.FileInputStream
import java.util.Properties

/**
 * Created by mharis on 22/09/15.
 *
 * This class is for submitting the job with `./submit` script, e.g.:
 *
 * ./submit net.imagini.dxp.graphstream.ingest.SyncsToGraph 3 /etc/vdna/graphstream/config.properties
 */
object SyncsToGraph {
  def main(args: Array[String]) = {
    val config = new Properties
    config.load( new FileInputStream(args(1)))
    new SyncsToGraphApplication(config)
      .runOnYarn(taskMemoryMb = args(0).toInt * 1024, awaitCompletion = args.length == 3 && args(2) == "wait")
  }
}
