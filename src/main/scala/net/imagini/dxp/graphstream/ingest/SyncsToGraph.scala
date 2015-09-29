package net.imagini.dxp.graphstream.ingest

import java.io.FileInputStream
import java.util.Properties

/**
 * Created by mharis on 22/09/15.
 *
 * This class is for submitting the job with `./submit` script, e.g.:
 *
 * ./submit net.imagini.dxp.graphstream.ingest.SyncsToGraph /etc/vdna/graphstream/config.properties
 */
object SyncsToGraph {
  def main(args: Array[String]) = {
    val config = new Properties
    config.load( new FileInputStream(args(0)))
    new SyncsToGraphApplication(config)
      .runOnYarn(taskMemoryMb = 8 * 1024, awaitCompletion = args.length == 2 && args(1) == "wait")
  }
}