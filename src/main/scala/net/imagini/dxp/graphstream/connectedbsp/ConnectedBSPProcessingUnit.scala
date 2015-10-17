package net.imagini.dxp.graphstream.connectedbsp

import java.net.URL
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import kafka.producer.KeyedMessage
import net.imagini.dxp.common.VidKafkaPartitioner
import org.apache.donut._
import org.apache.donut.memstore.MemStoreLogMap
import org.apache.donut.metrics.{Counter, Ratio, Throughput}
import org.apache.donut.utils.logmap.ConcurrentLogHashMap

/**
 * Created by mharis on 14/09/15.
 */
class ConnectedBSPProcessingUnit(
  config: Properties, trackingUrl: URL, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, trackingUrl, logicalPartition, totalLogicalPartitions, topics) {

  type MESSAGE = KeyedMessage[ByteBuffer, ByteBuffer]

  val evictions = new AtomicLong(0)
  val stateIn = new AtomicLong(0)
  val stateInvalid = new AtomicLong(0)
  val stateOut = new AtomicLong(0)
  val deltaIn = new AtomicLong(0)
  val deltaInvalid = new AtomicLong(0)
  val deltaOut = new AtomicLong(0)

  val debug = config.getProperty("debug", "false").toBoolean
  if (debug) println("DEBUG MODE ENABLED!")

  val maxMemoryMemstoreMb = config.getProperty("direct.memory.mb").toInt / totalLogicalPartitions - 128

  private val logmap = new ConcurrentLogHashMap(
    maxMemoryMemstoreMb,
    segmentSizeMb = 100,
    compressMinBlockSize = 131070,
    indexLoadFactor = 0.87) {
    /**
     * When the in-memory state overflows we also create a tombstone in the compacted state topic
     * Since the producers must be expected to be asynchronous we have to make a copy of the key
     * buffer
     */
    override def onEvictEntry(key: ByteBuffer): Unit = {
      if (key == null || key.remaining <= 0) {
        throw new IllegalArgumentException("Key cannot be null or empty")
      }
      evictions.incrementAndGet
      produce(List(
        new KeyedMessage("graphstate", key, null.asInstanceOf[ByteBuffer]),
        new KeyedMessage("graphdelta", key, null.asInstanceOf[ByteBuffer])
      ))
    }
  }

  private val processor = new ConnectedBSPProcessor(minEdgeProbability = 0.75, new MemStoreLogMap(logmap))

  @volatile var ts = System.currentTimeMillis

  override def awaitingTermination {
    val period = (System.currentTimeMillis - ts)
    ts = System.currentTimeMillis
    ui.updateMetric("gs:in/sec", classOf[Throughput], stateIn.getAndSet(0) * 1000 / period)
    ui.updateMetric("gs:invalid", classOf[Counter], stateInvalid.get)
    ui.updateMetric("gs:out/sec", classOf[Throughput], stateOut.getAndSet(0) * 1000 / period)

    ui.updateMetric("st:size", classOf[Counter], processor.memstore.size)
    ui.updateMetric("st:evictions/sec", classOf[Throughput], evictions.getAndSet(0) * 1000 / period)
    ui.updateMetric("st:memory.mb", classOf[Ratio],
      value = s"${processor.memstore.sizeInBytes / 1024 / 1024}/${maxMemoryMemstoreMb}",
      hint = s"${processor.memstore.stats(true).mkString("\n")}")
    ui.updateMetric("st:bsp-miss", classOf[Counter], processor.bspMiss.get)
    ui.updateMetric("st:bsp-over", classOf[Counter], processor.bspOverflow.get)

    ui.updateMetric("d:in/sec", classOf[Throughput], deltaIn.getAndSet(0) * 1000 / period)
    ui.updateMetric("d:invalid/sec", classOf[Throughput], deltaInvalid.getAndSet(0))
    ui.updateMetric("d:out/sec", classOf[Throughput], deltaOut.getAndSet(0) * 1000 / period)
  }

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
                produce(List(
                  new KeyedMessage("graphstate", messageAndOffset.message.key, null)
                ))
              }
            }
          }
        }
      }

      case "graphdelta" => new FetcherDelta(this, topic, partition, groupId) {
        override def handleMessage(envelope: MessageAndOffset): Unit = {
          try {
            val outputMessages = processor.processDeltaInput(envelope.message.key, envelope.message.payload)
            deltaIn.incrementAndGet
            produce(outputMessages)
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

  /**
   * Transparently produce messages of type (ByteBuffer, ByteBuffer).
   *
   * @param outputMessages
   */
  def produce(outputMessages: List[MESSAGE]): Unit = {
    outputMessages.foreach(message => {
      message.topic match {
        case "graphdelta" => {
          if (!debug) deltaProducer.send(message)
          deltaOut.incrementAndGet
        }
        case "graphstate" => {
          if (!debug) stateProducer.send(message)
          stateOut.incrementAndGet
        }
      }
    })
  }

  private val deltaProducer = kafkaUtils.snappySyncProducer[VidKafkaPartitioner](numAcks = -1)
  private val stateProducer = kafkaUtils.compactSyncProducer[VidKafkaPartitioner](numAcks = -1) //batchSize = 500

  override def onShutdown: Unit = {
    deltaProducer.close
    stateProducer.close
  }




}

