package net.imagini.dxp.graphstream.ingest

import java.io.FileInputStream
import java.nio.ByteBuffer
import java.util.Properties

import kafka.producer.{KeyedMessage, Producer}
import net.imagini.dxp.common._
import org.apache.donut.KafkaUtils

import scala.io.Source

/**
 * Created by mharis on 01/10/15.
 *
 * This class is for dumping syncs and adjacency lists from a standard input into the graphdelta topic,
 * essentially importing graph connection from files.
 *

DATE="2015-09-25"; gunzip -c /storage/fileshare/.. | \
java -cp graphstream-0.9.jar:/opt/scala/scala-library-2.10.4.jar:/opt/scala/kafka_2.10-0.8.2.1.jar \
net.imagini.dxp.graphstream.ingest.FileToGraph /etc/vdna/graphstream/config.properties "$DATE"

 */

object FileToGraph extends App {

  val config = new Properties
  config.load(new FileInputStream(args(0)))
  val date = args(1)
  val probabilityThreshold = if (args.length >= 3) args(2).toDouble else 0.75
  val mobileIdSpace = if (args.length >= 4) args(3) else "*"
  try {
    new FileToGraph(config, date, mobileIdSpace, probabilityThreshold).processStdIn
  } catch {
    case e: Throwable => {
      e.printStackTrace()
      System.exit(1)
    }
  }

}

class FileToGraph(config: Properties, val date: String, val mobileIdSpace: String, val probabilityThreshold: Double) {

  val kafkaUtils = new KafkaUtils(config)
  val decoder = new CWDecoder(date, mobileIdSpace, probabilityThreshold)
  var producer: Producer[ByteBuffer, ByteBuffer] = null

  def processStdIn: Unit = {

    println(s"Preparing CWDecoder for DATE = ${date}, mobile space = ${mobileIdSpace}, probability >= ${probabilityThreshold}\n")

    var counterIgnored = 0L
    var counterInvalid = 0L
    var counterProduced = 0L

    for (ln <- Source.stdin.getLines) {
      try {
        processLine(ln) match {
          case None => {
            counterIgnored += 1L
            if (counterIgnored % 100000 == 0) {
              println(s"IGNORED LINES: ${counterIgnored} ...")
            }
          }
          case Some(couple) => {
            if (producer == null) {
              producer = kafkaUtils.createSnappyProducer[VidKafkaPartitioner](numAcks = 1, batchSize = 500)
            }
            producer.send(
              new KeyedMessage(
                "graphdelta",
                ByteBuffer.wrap(BSPMessage.encodeKey(couple(0)._1)),
                ByteBuffer.wrap(BSPMessage.encodePayload((1, Map(couple(0)._2))))),
              new KeyedMessage(
                "graphdelta",
                ByteBuffer.wrap(BSPMessage.encodeKey(couple(1)._1)),
                ByteBuffer.wrap(BSPMessage.encodePayload((1, Map(couple(1)._2)))))
              )
            counterProduced += 2L
            if (counterProduced % 100000 == 0) {
              println(s"PRODUCED MESSAGES: ${counterProduced} ...")
            }
          }
        }
      } catch {
        case e: IllegalArgumentException => counterInvalid += 1L
      }
    }

    println(s"PRODUCED MESSAGES: ${counterProduced}")
    println(s"IGNORED LINES: ${counterIgnored}")
    println(s"PRODUCED MESSAGES: ${counterInvalid}")

  }

  def processLine(ln: String): Option[List[(Vid, (Vid, Edge))]] = {
    val pair: (Vid, (Vid, Edge)) = decoder.decodeTextLine(ln)
    if (pair._1 != null && pair._2._1 != null) {
      Some(List(pair, (pair._2._1, (pair._1 -> pair._2._2))))
    } else {
      None
    }
  }

}
