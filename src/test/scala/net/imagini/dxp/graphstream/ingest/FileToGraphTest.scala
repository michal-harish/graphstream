package net.imagini.dxp.graphstream.ingest

import net.imagini.dxp.graphstream.Config
import org.scalatest.Matchers
import org.scalatest.FlatSpec

import scala.io.Source

/**
 * Created by mharis on 01/10/15.
 */
class FileToGraphTest extends FlatSpec with Matchers {

  val component = new FileToGraph(Config, "2015-09-25", mobileIdSpace = "idfa", probabilityThreshold = 1.0)

  val syncs = Source.fromInputStream(getClass.getResourceAsStream("/sample_cw.tsv")).getLines.flatMap { ln =>
    component.processLine(ln) match {
      case None => List()
      case Some(couple) => couple
    }
  }
  syncs.size should be  (48)

}
