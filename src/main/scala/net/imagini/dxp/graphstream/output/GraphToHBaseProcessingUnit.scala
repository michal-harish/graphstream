package net.imagini.dxp.graphstream.output

import java.io.IOException
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, Semaphore}

import io.amient.donut.metrics.Throughput
import io.amient.donut.{DonutAppTask, Fetcher, FetcherDelta}
import kafka.message.MessageAndOffset
import net.imagini.dxp.common.{BSPMessage, Edge, Vid}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 28/09/15.
 *
 * This is a version 2 of the Stream-to-HBase loader for graph state. It uses semaphore with the counter equal
 * to the number of fetchers which all can proceed until the compaction thread kicks in and acquires the total number
 * and turns 'red'.
 */
class GraphToHBaseProcessingUnit(config: Properties, args: Array[String]) extends DonutAppTask(config, args) {

  private val log = LoggerFactory.getLogger(classOf[GraphToHBaseProcessingUnit])

  val MIN_COMPACTION_SIZE = 1000
  val MAX_COMPACTION_SLEEP_TIME_MS = 1000L
  val MAX_NUM_HBASE_RETRIES = 5

  val hbaConf: Configuration = tryOrReport(HBaseConfiguration.create())
  tryOrReport(hbaConf.addResource(new Path(s"file://${config.getProperty("yarn1.site")}/core-site.xml")))
  tryOrReport(hbaConf.addResource(new Path(s"file://${config.getProperty("yarn1.site")}/hdfs-site.xml")))
  tryOrReport(hbaConf.addResource(new Path(s"file://${config.getProperty("yarn1.site")}/yarn-site.xml")))
  tryOrReport(hbaConf.addResource(new Path(s"file://${config.getProperty("hbase.site")}/hbase-site.xml")))

  log.info("HBase configuration from " + config.getProperty("hbase.site") + "/hbase-site.xml")
  log.info("hbase.zookeeper.quorum = " + hbaConf.get("hbase.zookeeper.quorum"))

  private val compactionSemaphore = tryOrReport(new Semaphore(numFetchers))

  private val compactedQueue = tryOrReport(new ConcurrentHashMap[Vid, java.util.Map[Vid, Edge]])

  private val deltaCounter = new AtomicLong(0)

  private val deleteCounter = new AtomicLong(0)

  private val putCounter = new AtomicLong(0)

  private var connection: Connection = null

  private var table: BufferedMutator = null

  private var loaderThread: LoaderThread = null

  private val tableNameAsString = tryOrReport(config.getProperty("hbase.table"))

  @volatile private var ts = System.currentTimeMillis

  override def awaitingTermination: Unit = {
    val period = (System.currentTimeMillis - ts)
    ts = System.currentTimeMillis
    ui.updateMetric(partition, "input graphdelta/sec", classOf[Throughput], deltaCounter.getAndSet(0) * 1000 / period)
    ui.updateMetric(partition, s"output ${tableNameAsString} put/sec", classOf[Throughput], putCounter.getAndSet(0) * 1000 / period)
    ui.updateMetric(partition, s"output ${tableNameAsString} del/sec", classOf[Throughput], deleteCounter.getAndSet(0) * 1000 / period)
  }

  override protected def onShutdown: Unit = {
    closeTable
    if (loaderThread != null) {
      loaderThread.interrupt
      loaderThread.join
    }
  }

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "graphdelta" => new FetcherDelta(this, topic, partition, groupId) {
        override protected def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          val key = BSPMessage.decodeKey(messageAndOffset.message.key)
          val payload = messageAndOffset.message.payload
          deltaCounter.incrementAndGet
          compactionSemaphore.acquire(1)
          try {
            if (loaderThread == null) {
              loaderThread = new LoaderThread
              loaderThread.start
            }
            if (payload == null) {
              compactedQueue.put(key, java.util.Collections.emptyMap[Vid, Edge])
            } else {
              val addedEdges = BSPMessage.decodePayload(payload)._2
              if (!compactedQueue.containsKey(key)) {
                compactedQueue.put(key, addedEdges)
              } else compactedQueue.get(key) match {
                case previousEdges if (previousEdges.size == 0) => compactedQueue.put(key, addedEdges)
                case previousEdges => previousEdges.putAll(addedEdges)
              }
            }
          } finally {
            compactionSemaphore.release(1)
          }
        }
      }
    }
  }

  private class LoaderThread extends Thread {
    override def run: Unit = {
      while (!isInterrupted) {
        if (compactedQueue.size < MIN_COMPACTION_SIZE) {
          Thread.sleep(MAX_COMPACTION_SLEEP_TIME_MS)
        } else {
          var numErrors = 0L
          compactionSemaphore.acquire(numFetchers)
          try {
            while (!isInterrupted && compactedQueue.size > 0) {
              try {
                if (connection == null) {
                  log.info("Opening HBase connection")
                  connection = ConnectionFactory.createConnection(hbaConf)
                  log.info(s"Opening HBase Buffered Mutator for table `${tableNameAsString}")
                  table = connection.getBufferedMutator(TableName.valueOf(tableNameAsString))
                }
                log.debug(s"HBase mutation size = ${compactedQueue.size}")
                val it = compactedQueue.entrySet.iterator
                while (!isInterrupted && it.hasNext) {
                  val entry = it.next
                  val key = entry.getKey
                  val edges = entry.getValue
                  var put: Put = null
                  var delete: Delete = null
                  if (edges.size == 0) {
                    delete = new Delete(key.bytes)
                    delete.addFamily(Bytes.toBytes("N"))
                  } else {
                    val it = edges.entrySet.iterator
                    while (it.hasNext) {
                      val i = it.next
                      val destVid = i.getKey
                      val destEdge = i.getValue
                      if (destEdge.probability == 0) {
                        if (delete == null) {
                          delete = new Delete(key.bytes)
                        }
                        delete.addColumn(Bytes.toBytes("N"), destVid.bytes)
                      } else {
                        if (put == null) {
                          put = new Put(key.bytes)
                        }
                        put.addColumn(Bytes.toBytes("N"), destVid.bytes, destEdge.ts, destEdge.bytes)
                      }
                    }
                  }
                  if (delete != null && !delete.isEmpty) {
                    deleteCounter.addAndGet(delete.size)
                    table.mutate(delete)
                  }
                  if (put != null && !put.isEmpty) {
                    putCounter.addAndGet(put.size)
                    table.mutate(put)
                  }
                }
                table.flush

                compactedQueue.clear
              } catch {
                case e: IOException => {
                  closeTable
                  numErrors += 1
                  log.warn(s"HBase mutation retry: ${numErrors}, mutation size = ${compactedQueue.size}", e)
                  if (numErrors >= MAX_NUM_HBASE_RETRIES) {
                    propagateException(e)
                    return
                  }
                }
              }
            }
          } finally {
            compactionSemaphore.release(numFetchers)
          }
        }
      }
    }
  }

  private def closeTable = {
    if (connection != null) try {
      log.info("Closing HBase connection")
      connection.close
      table.close
    } catch {
      case e: IOException => {}
    } finally {
      connection = null
      table = null
    }
  }


}
