package net.imagini.dxp.graphstream.debugging

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import kafka.producer.KeyedMessage
import net.imagini.dxp.common.VidKafkaPartitioner
import org.apache.donut.memstore.MemStoreLogMap
import org.apache.donut.utils.logmap.ConcurrentLogHashMap
import org.apache.donut.{DonutAppTask, Fetcher, FetcherBootstrap}

/**
 * Created by mharis on 14/10/15.
 */
class GraphStateCompactorProcessor(config: Properties, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  val maxStateSizeMb = config.getProperty("direct.memory.mb").toInt / totalLogicalPartitions

  println(s"MAX STATE SIZE IN MB = ${maxStateSizeMb}")

  private val logmap = new ConcurrentLogHashMap(
    maxSizeInMb = config.getProperty("direct.memory.mb").toInt / totalLogicalPartitions - 128,
    segmentSizeMb = 100,
    compressMinBlockSize = 131070,
    indexLoadFactor = 0.87) {
    override def onEvictEntry(key: ByteBuffer): Unit = {
      /**
       * When the in-memory state overflows we also create a tombstone in the compacted state topic
       */
      evicted.incrementAndGet
      stateProducer.send(
        new KeyedMessage("graphstate", key, null.asInstanceOf[ByteBuffer]),
        new KeyedMessage("graphdelta", key, null.asInstanceOf[ByteBuffer])
      )
    }
  }
  val altstore = new MemStoreLogMap(logmap)

  val altmap = altstore.map
  var ts = System.currentTimeMillis
  val stateIn = new AtomicLong(0)
  val evicted = new AtomicLong(0)
  val invalid = new AtomicLong(0)

  private val stateProducer = kafkaUtils.createCompactProducer[VidKafkaPartitioner](numAcks = 0, batchSize = 200)

  override def executeCommand(cmd: String): Unit = {
    val c = cmd.split("\\s+").iterator
    c.next match {
      case "" => println()
      case "exit" => propagateException(new InterruptedException)
      //case "ui" =>
      //case "compress" => altmap.applyCompression(c.next.toDouble)
      case any => println("Usage:" +
        "\n\t[ENTER]\t\tprint basic stats" +
        "\n\texit\t\tclose the application" +
        "\n\tui\t\tget web ui url" +
        "\n\tcompress <fraction>\t\tcompress any segments in the tail of the log that occupies more than <fraction> of total hash map memory")
    }
    altmap.printStats(true)
  }

  override protected def awaitingTermination: Unit = {
    val t = (System.currentTimeMillis - ts)
    ts = System.currentTimeMillis
    val s = stateIn.getAndSet(0) * 1000 / t
    val e = evicted.getAndSet(0) * 1000 / t
    val i = invalid.getAndSet(0) * 1000 / t
    println(s"graphstate ${s}/sec,  evicted = ${e}/sec, invalid = ${i}/sec")
    logmap.printStats(false)
  }

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "graphstate" => new FetcherBootstrap(this, topic, partition, groupId) {
        def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          stateIn.incrementAndGet
          buildState(messageAndOffset.message.key, messageAndOffset.message.payload)
          Thread.sleep(100)
        }
      }
    }
  }

  override protected def onShutdown: Unit = {}

  def buildState(msgKey: ByteBuffer, payload: ByteBuffer) = {
    try {
      altstore.put(msgKey, payload)
    } catch {
      case e: IllegalArgumentException => invalid.incrementAndGet
    }
  }
}
