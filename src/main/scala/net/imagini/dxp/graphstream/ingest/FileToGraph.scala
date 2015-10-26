package net.imagini.dxp.graphstream.ingest

import java.io.{IOException, FileInputStream}
import java.nio.ByteBuffer
import java.util.Properties

import io.amient.donut.KafkaUtils
import io.amient.utils.ByteUtils
import kafka.producer.{KeyedMessage, Producer}
import net.imagini.dxp.common._
import net.imagini.dxp.graphstream.connectedbsp.ConnectedGraphBSPStreaming

import scala.io.Source

/**
 * Created by mharis on 01/10/15.
 *
 * This class is for dumping syncs and adjacency lists from a standard input into the graphdelta topic,
 * essentially importing graph connection from files.
 *

DATE="2015-09-25"; gunzip -c /storage/fileshare/.. | \
java -cp graphstream-0.9.jar:/opt/scala/scala-library-2.11.5.jar:/opt/scala/kafka_2.11-0.8.2.1.jar \
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

class FileToGraph(
                   config: Properties,
                   val date: String,
                   val mobileIdSpace: String,
                   val probabilityThreshold: Double) {

  val kafkaUtils = new KafkaUtils(config)
  val decoder = new CWDecoder(date, mobileIdSpace, probabilityThreshold)
  var producer: Producer[Array[Byte], Array[Byte]] = null
  var checkpointNs = 0L
  val checkpointIntervalNs = 60L * 1000000000
  val msToBackOff = checkpointIntervalNs * 2 / 1000000

  def processStdIn: Unit = {

    println(s"Preparing CWDecoder for DATE = ${date}, " +
      s"mobile space = ${mobileIdSpace}, probability >= ${probabilityThreshold}\n")

    var counterIgnored = 0L
    var counterInvalid = 0L
    var counterProduced = 0L

    try {
      for (ln <- Source.stdin.getLines) {
        try {
          if (ln != null) processLine(ln) match {
            case None => counterIgnored += 1L
            case Some(couple) => {
              produce(couple(0))
              counterProduced += 1L
              produce(couple(1))
              counterProduced += 1L
            }
          }
          val ns = System.nanoTime
          if (checkpointNs + checkpointIntervalNs < ns) {
            println(s"IGNORED LINES: ${counterIgnored} ...")
            println(s"PRODUCED MESSAGES: ${counterProduced} ...")
            while (feedbackLoop) {
              Thread.sleep(msToBackOff)
            }
            checkpointNs = ns
          }
        } catch {
          case e: IllegalArgumentException => counterInvalid += 1L
        }
      }

    } finally {
      println(s"COMPLETED, PRODUCED MESSAGES: ${counterProduced}")
      println(s"COMPLETED, IGNORED LINES: ${counterIgnored}")
      println(s"COMPLETED, INVALID MESSAGES: ${counterInvalid}")
    }

  }

  def processLine(ln: String): Option[List[(Vid, (Vid, Edge))]] = {
    val pair: (Vid, (Vid, Edge)) = decoder.decodeTextLine(ln)
    if (pair._1 != null && pair._2._1 != null) {
      Some(List(pair, (pair._2._1, (pair._1 -> pair._2._2))))
    } else {
      None
    }
  }

  private def produce(pair: (Vid, (Vid, Edge))): Unit = {

    var numHardErrors = 0
    while (true) try {
      produce((
        BSPMessage.encodeKey(pair._1),
        BSPMessage.encodePayload(1, pair._2)))
      return
    } catch {
      case e: IOException => {
        numHardErrors += 1
        if (numHardErrors > 3) {
          throw e
        } else {
          if (producer != null) {
            try {
              producer.close
            } catch {
              case e: Throwable => {}
            }
          }
          producer = null
          Thread.sleep(5000)
        }
      }
    }
  }

  private def produce(message: (ByteBuffer, ByteBuffer)): Unit = {
    var numSoftErrors = 0
    while (true) try {
      if (producer == null) {
        producer = kafkaUtils.snappyAsyncProducer[VidKafkaPartitioner](numAcks = 0, batchSize = 500)
      }
      producer.send(new KeyedMessage("graphdelta",
        ByteUtils.bufToArray(message._1), ByteUtils.bufToArray(message._2)))
      return
    } catch {
      case e: IOException => {
        numSoftErrors += 1
        if (numSoftErrors > 3) {
          throw e
        } else {
          Thread.sleep(1000)
        }
      }
    }
  }

  private def feedbackLoop: Boolean = {
    var numErrors = 0
    var error: Throwable = null
    while (numErrors < 5) {
      try {
        val downstreamProgress = kafkaUtils.getGroupProgress(ConnectedGraphBSPStreaming.GROUP_ID, List("graphdelta"))
        val (minimum, average, maximum) = downstreamProgress

        val activate = minimum < 0.5 || average < 0.95
        println(s"DOWNSTREAM PROGRESS: Min = ${100 * minimum}%, Avg = ${100 * average}%" +
          (if (activate) s" WAITING ${msToBackOff / 1000} SECONDS.." else ""))
        return activate
      } catch {
        case e: IOException => {
          numErrors += 1
          error = e
          System.err.println(e.getMessage)
          Thread.sleep(msToBackOff)
        }
      }
    }
    throw error
  }


}
