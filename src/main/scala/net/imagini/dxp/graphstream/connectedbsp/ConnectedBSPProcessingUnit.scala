package net.imagini.dxp.graphstream.connectedbsp

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import io.amient.donut.memstore.MemStoreLogMap
import io.amient.donut.metrics.{Counter, Ratio, Throughput}
import io.amient.donut.{DonutAppTask, Fetcher, FetcherBootstrap, FetcherDelta}
import io.amient.utils.ByteUtils
import io.amient.utils.logmap.ConcurrentLogHashMap
import kafka.message.MessageAndOffset
import kafka.producer.KeyedMessage
import net.imagini.dxp.common.{BSPMessage, VidKafkaPartitioner}

/**
 * Created by mharis on 14/09/15.
 */
class ConnectedBSPProcessingUnit(config: Properties, args: Array[String]) extends DonutAppTask(config, args) {

  type MESSAGE = KeyedMessage[ByteBuffer, ByteBuffer]

  val evictions = new AtomicLong(0)
  val stateIn = new AtomicLong(0)
  val stateInvalid = new AtomicLong(0)
  val stateOut = new AtomicLong(0)
  val deltaIn = new AtomicLong(0)
  val deltaInThroughput = new AtomicLong(0)
  val deltaWaste = new AtomicLong(0)
  val deltaInvalid = new AtomicLong(0)
  val deltaOutThroughput = new AtomicLong(0)
  val deltaOut = new AtomicLong(0)

  val debug = config.getProperty("debug", "false").toBoolean
  if (debug) println("BSP DEBUG MODE ENABLED!")

  val maxMemoryMemstoreMb = config.getProperty("direct.memory.mb").toInt / numPartitions - 128

  private val logmap = new ConcurrentLogHashMap(
    maxMemoryMemstoreMb,
    segmentSizeMb = 100,
    compressMinBlockSize = 131070,
    indexLoadFactor = 0.7) {
    /**
     * When the in-memory state overflows we also create a tombstone in the compacted state topic
     * Since the producers must be expected to be asynchronous we have to make a copy of the key
     * buffer
     */
    override def onEvictEntry(key: ByteBuffer): Unit = {
      evictions.incrementAndGet
      produce(List(
        new KeyedMessage("graphstate", key, null.asInstanceOf[ByteBuffer]),
        new KeyedMessage("graphdelta", key, null.asInstanceOf[ByteBuffer])
      ))
    }
  }

  private val memstore = new MemStoreLogMap(logmap)

//  private val server = new MemStoreServer(memstore)
//
//  server.start
//
//  ui.updateStatus(partition, "memstore address", (AddressUtils.getLANHost() + ":" + server.getListeningPort))

  /**
   * The processor is a separate class for testing purpose. The processing unit provides the
   * integration with the mssaging infrastructure, the processor is a stateless logic.
   */
  private val processor = new ConnectedBSPProcessor(minEdgeProbability = 0.75, memstore)

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "graphstate" => new FetcherBootstrap(this, topic, partition, groupId) {
        def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          if (!isBooted) {
            try {
              processor.bootState(messageAndOffset.message.key, messageAndOffset.message.payload)
              stateIn.incrementAndGet
            } catch {
              case e: IllegalArgumentException => {
                if (debug) e.printStackTrace
                stateInvalid.incrementAndGet
                if (messageAndOffset.message.key != null) {
                  stateProducer.send(new KeyedMessage("graphstate", ByteUtils.bufToArray(messageAndOffset.message.key), null))
                }
              }
            }
          }
        }
      }

      case "graphdelta" => new FetcherDelta(this, topic, partition, groupId) {
        override def handleMessage(envelope: MessageAndOffset): Unit = {
          try {
            if (envelope.message.payload != null) {
              val outputMessages = processor.processDeltaInput(envelope.message.key, envelope.message.payload)
              deltaIn.incrementAndGet
              deltaInThroughput.incrementAndGet
              if (outputMessages.size == 0) {
                deltaWaste.incrementAndGet
              } else {
                produce(outputMessages)
              }
            }
          } catch {
            case e: IllegalArgumentException => {
              if (debug) e.printStackTrace
              deltaInvalid.incrementAndGet
            }
          }
        }
      }
    }
  }

  @volatile var ts = System.currentTimeMillis
  override def awaitingTermination {
    val period = (System.currentTimeMillis - ts)
    ts = System.currentTimeMillis
    ui.updateMetric(partition, "input state/sec", classOf[Throughput], stateIn.getAndSet(0) * 1000 / period)
    ui.updateMetric(partition, "input state-error", classOf[Counter], stateInvalid.get)
    ui.updateMetric(partition, "output state/sec", classOf[Throughput], stateOut.getAndSet(0) * 1000 / period)

    ui.updateMetric(partition, "memstore bsp-miss", classOf[Counter], processor.bspMiss.get)
    ui.updateMetric(partition, "memstore bsp-over", classOf[Counter], processor.bspOverflow.get)
    ui.updateMetric(partition, "memstore evictions", classOf[Throughput], evictions.get)
    ui.updateMetric(partition, "memstore memory.mb", classOf[Ratio],
      value = s"${processor.memstore.sizeInBytes / 1024 / 1024}/${maxMemoryMemstoreMb}",
      hint = s"${processor.memstore.stats(true).mkString("\n")}")
    ui.updateMetric(partition, "memstore size", classOf[Counter], processor.memstore.size)

    ui.updateMetric(partition, "input delta/sec", classOf[Throughput], deltaInThroughput.getAndSet(0) * 1000 / period)
    ui.updateMetric(partition, "input delta-total", classOf[Counter], deltaIn.get)
    ui.updateMetric(partition, "input delta-waste", classOf[Counter], deltaWaste.get)
    ui.updateMetric(partition, "input delta-errors", classOf[Counter], deltaInvalid.get)
    ui.updateMetric(partition, "output delta-total", classOf[Counter], deltaOut.get)
    ui.updateMetric(partition, "output delta/sec", classOf[Throughput], deltaOutThroughput.getAndSet(0) * 1000 / period)
  }


  /**
   * Transparently produce messages of type (ByteBuffer, ByteBuffer).
   *
   * @param outputMessages
   */
  def produce(outputMessages: List[MESSAGE]): Unit = {
    outputMessages.foreach(message => {
      if (message.key == null || message.key.remaining <= 0) {
        throw new IllegalArgumentException(s"Key cannot be null or empty: ${message.key}, topic `${message.topic}` " +
          s", payload = ${BSPMessage.decodePayload(message.message)}")
      }

      val byteMessage = new KeyedMessage[Array[Byte], Array[Byte]](
        message.topic, ByteUtils.bufToArray(message.key), ByteUtils.bufToArray(message.message))

      message.topic match {
        case "graphdelta" => {
          if (!debug) deltaProducer.send(byteMessage)
          deltaOutThroughput.incrementAndGet
          deltaOut.incrementAndGet
        }
        case "graphstate" => {
          if (!debug) stateProducer.send(byteMessage)
          stateOut.incrementAndGet
        }
      }
    })
  }

  private val deltaProducer = kafkaUtils.snappyAsyncProducer[VidKafkaPartitioner](numAcks = 0, batchSize = 500)
  private val stateProducer = kafkaUtils.compactAsyncProducer[VidKafkaPartitioner](numAcks = 0, batchSize = 500)

  override def onShutdown: Unit = {
    deltaProducer.close
    stateProducer.close
  }


}

