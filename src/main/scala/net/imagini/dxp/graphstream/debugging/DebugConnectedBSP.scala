package net.imagini.dxp.graphstream.debugging

import java.io.FileInputStream
import java.util.Properties

/**
 * Created by mharis on 14/10/15.
 */
object DebugConnectedBSP {
  def main(args: Array[String]) = {
    val config = new Properties
    config.load(new FileInputStream(args(0)))
    new DebugConnectedBSPApplication(config).runOnYarn(awaitCompletion = args.length == 2 && args(1) == "wait")
  }
}
