package net.imagini.dxp.graphstream.ingest

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import io.amient.donut.metrics.{Counter, Throughput}
import io.amient.donut.{DonutAppTask, Fetcher, FetcherDelta}
import io.amient.utils.ByteUtils
import kafka.message.MessageAndOffset
import kafka.producer.KeyedMessage
import net.imagini.common.message.VDNAUserImport
import net.imagini.common.messaging.serde.VDNAUniversalDeserializer
import net.imagini.dxp.common._

/**
 * Created by mharis on 15/09/15.
 */
class SyncsToGraphProcessingUnit(config: Properties, args: Array[String]) extends DonutAppTask(config, args) {

  val counterRaw = new AtomicLong(0)
  val counterValid = new AtomicLong(0)
  val counterErrors = new AtomicLong(0)
  val counterFiltered = new AtomicLong(0)
  val counterProcess = new AtomicLong(0)
  val counterProduced = new AtomicLong(0)
  val idSpaceSet = Set("a", "r", "d")

  val vdnaMessageDecoder = tryOrReport(new VDNAUniversalDeserializer)

  val blacklists = tryOrReport(new BlackLists)

  val producer = tryOrReport(kafkaUtils.createProducer[VidKafkaPartitioner]("snappy_producer"))

  @volatile private var ts = System.currentTimeMillis

  override def awaitingTermination: Unit = {
    val period = (System.currentTimeMillis - ts)
    ts = System.currentTimeMillis
    ui.updateMetric(partition, "input (1) VDNAUserImport/sec", classOf[Throughput], counterRaw.getAndSet(0) * 1000 / period)
    ui.updateMetric(partition, "input (2) valid/sec", classOf[Throughput], counterValid.getAndSet(0) * 1000 / period)
    ui.updateMetric(partition, "input (3) filter/sec", classOf[Throughput], counterFiltered.getAndSet(0) * 1000 / period)
    ui.updateMetric(partition, "input (4) process/sec", classOf[Throughput], counterProcess.getAndSet(0) * 1000 / period)
    ui.updateMetric(partition, "output errors", classOf[Counter], counterErrors.get)
    ui.updateMetric(partition, "output graphstream/sec", classOf[Throughput], counterProduced.getAndSet(0) * 1000 / period)
  }

  override def onShutdown: Unit = {
    producer.close
  }

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "datasync" => new FetcherDelta(this, topic, partition, groupId) {
        override def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          val payload = messageAndOffset.message.payload
          //TODO now that we have ByteBuffers vdna decoder should support offset to deserialize from
          val payloadArray: Array[Byte] = util.Arrays.copyOfRange(payload.array, payload.arrayOffset, payload.arrayOffset + payload.remaining)
          val vdnaMsg = vdnaMessageDecoder.decodeBytes(payloadArray)
          if (vdnaMsg.isInstanceOf[VDNAUserImport]) {
            counterRaw.incrementAndGet
            val importMsg = vdnaMsg.asInstanceOf[VDNAUserImport]
            if (importMsg.getUserCookied &&
              !importMsg.getUserOptOut &&
              importMsg.getUserUid != null &&
              importMsg.getPartnerUserId != null
            ) {
              counterValid.addAndGet(1L)
              if (importMsg.getIdSpace() == "a") {
                println("a")
              }
              if (importMsg.getIdSpace() == "d") {
                println("d")
              }
              if (idSpaceSet.contains(importMsg.getIdSpace())) {
                counterFiltered.addAndGet(1L)
                if ((importMsg.getUserAgent == null || !blacklists.blacklist_ua.contains(importMsg.getUserAgent.trim.hashCode))
                  && !blacklists.blacklist_vdna_uuid.contains(importMsg.getUserUid)
                  && !blacklists.blacklist_id.contains(importMsg.getPartnerUserId)
                  && (importMsg.getClientIp == null || !blacklists.blacklist_ip.contains(importMsg.getClientIp.trim.hashCode))
                ) {
                  counterProcess.addAndGet(1L)
                  transformAndProduce(importMsg)
                }
              }
            }
          }
        }
      }
    }
  }

  def transformAndProduce(importMsg: VDNAUserImport) = {
    try {
      val vdnaId = Vid("vdna", importMsg.getUserUid.toString)
      val partnerId = Vid(importMsg.getIdSpace, importMsg.getPartnerUserId)
      val edge = Edge("AAT", 1.0, importMsg.getTimestamp)
      producer.send(
        new KeyedMessage(
          "graphdelta",
          ByteUtils.bufToArray(BSPMessage.encodeKey(vdnaId)),
          ByteUtils.bufToArray(BSPMessage.encodePayload(1, partnerId -> edge))),
        new KeyedMessage(
          "graphdelta",
          ByteUtils.bufToArray(BSPMessage.encodeKey(partnerId)),
          ByteUtils.bufToArray(BSPMessage.encodePayload(1, vdnaId -> edge)))
      )
      counterProduced.addAndGet(2L)
    } catch {
      case e: IllegalArgumentException => counterErrors.incrementAndGet
    }
  }

}
