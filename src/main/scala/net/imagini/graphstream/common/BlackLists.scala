package net.imagini.graphstream.common

import java.util.UUID

import scala.io.Source

/**
 * Created by mharis on 24/09/15.
 */
class BlackLists {

  //TODO build ip and ua exclusions from latest rd-pipe data instead of hardcoded resources
  val blacklist_ip: Seq[Int] = Source.fromURL(getClass.getResource("/ipExclude.txt")).getLines.map(_.trim.hashCode).toSeq
  val blacklist_ua: Seq[Int] = Source.fromURL(getClass.getResource("/ua.txt")).getLines.map(_.trim.hashCode).toSeq
  val blacklist_id: Seq[String] = Source.fromURL(getClass.getResource("/blacklist")).getLines.toSeq
  val blacklist_vdna: Seq[String] = Source.fromURL(getClass.getResource("/blacklist_vdna")).getLines.map(line => line.split(":")(1)).toSeq
  //TODO provide dynamic way of getting the list of top sellers
  val blacklist_vdna_uuid = blacklist_vdna.map(UUID.fromString(_))

}
