package net.imagini.dxp.graphstream.output

import java.io.FileInputStream
import java.util.Properties

/**
 * Created by mharis on 28/09/15.
 *
 * This class is for submitting the job with `./submit` script, e.g.:
 *
 * hbase ./submit net.imagini.dxp.graphstream.output.GraphToHBase 3 /etc/vdna/graphstream/config.properties
 */
object GraphToHBase {
  def main(args: Array[String]) = {
    val config = new Properties
    config.load( new FileInputStream(args(1)))
    new GraphToHBaseApplication(config)
      .runOnYarn(taskMemoryMb = args(0).toInt * 1024, awaitCompletion = args.length == 3 && args(2) == "wait")
  }
}
