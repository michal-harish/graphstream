package net.imagini.dxp.graphstream.output

import java.io.IOException
import java.util.Properties
import java.util.concurrent.{Semaphore, ConcurrentHashMap}

import kafka.message.MessageAndOffset
import net.imagini.dxp.common.{BSPMessage, Edge, Vid}
import org.apache.donut.{DonutAppTask, Fetcher, FetcherDelta}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 28/09/15.
 *
 * This is a version 2 of the Stream-to-HBase loader for graph state. It uses semaphore with the counter equal
 * to the number of fetchers which all can proceed until the compaction thread kicks in and acquires the total number
 * and turns 'red'.
 */
class GraphToHBaseProcessingUnitV2(config: Properties, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  private val log = LoggerFactory.getLogger(classOf[GraphToHBaseProcessingUnitV1])

  val MIN_COMPACTION_SIZE = 1000
  val MAX_COMPACTION_SLEEP_TIME_MS = 5000L
  val MAX_NUM_HBASE_RETRIES = 5

  val hbaConf = HBaseConfiguration.create()
  hbaConf.addResource(new Path(s"file://${config.getProperty("yarn1.site")}/core-site.xml"))
  hbaConf.addResource(new Path(s"file://${config.getProperty("yarn1.site")}/hdfs-site.xml"))
  hbaConf.addResource(new Path(s"file://${config.getProperty("yarn1.site")}/yarn-site.xml"))
  hbaConf.addResource(new Path(s"file://${config.getProperty("hbase.site")}/hbase-site.xml"))

  log.info("HBase configuration from " + config.getProperty("hbase.site") + "/hbase-site.xml")
  log.info("hbase.zookeeper.quorum = " + hbaConf.get("hbase.zookeeper.quorum"))

  private val compactionSemaphore = new Semaphore(numFetchers)

  private val compactedQueue = new ConcurrentHashMap[Vid, Map[Vid, Edge]]

  private var mutationCounter = 0L

  private var connection: Connection = null

  private var table: BufferedMutator = null

  private var loaderThread: LoaderThread = null

  private val tableNameAsString = config.getProperty("hbase.table")

  override protected def awaitingTermination: Unit = {
    println(s"TOTAL MUTATIONS IN TABLE `${tableNameAsString}` = ${mutationCounter}")
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
          compactionSemaphore.acquire(1)
          try {
            if (loaderThread == null) {
              loaderThread = new LoaderThread
              loaderThread.start
            }
            if (payload == null) {
              compactedQueue.put(key, null)
            } else {
              val addedEdges = BSPMessage.decodePayload(payload.array, payload.arrayOffset)._2
              if (!compactedQueue.containsKey(key)) {
                compactedQueue.put(key, addedEdges)
              } else compactedQueue.get(key) match {
                case null => {}
                case previousEdges => {
                  compactedQueue.put(key, previousEdges ++ addedEdges)
                }
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
        compactionSemaphore.acquire(numFetchers)
        var numErrors = 0L
        try {
          if (compactedQueue.size < MIN_COMPACTION_SIZE) {
            Thread.sleep(MAX_COMPACTION_SLEEP_TIME_MS)
          } else while (!isInterrupted && compactedQueue.size > 0) {
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
                if (edges == null) {
                  delete = new Delete(key.bytes)
                  delete.addFamily(Bytes.toBytes("N"))
                } else {
                  if (edges.size > 0) {
                    put = new Put(key.bytes)
                    edges.foreach { case (destVid, destEdge) => {
                      if (destEdge.probability == 0) {
                        if (delete == null) {
                          delete = new Delete(key.bytes)
                        }
                        delete.addColumn(Bytes.toBytes("N"), destVid.bytes)
                      } else {
                        put.addColumn(Bytes.toBytes("N"), destVid.bytes, destEdge.ts, destEdge.bytes)
                      }
                    }
                    }
                  }
                }
                if (delete != null) {
                  //TODO table.mutate(delete)
                }
                if (put != null) {
                  //TODO table.mutate(put)
                }
              }
              mutationCounter += compactedQueue.size
              compactedQueue.clear
            }
          }
        } catch {
          case e: IOException => {
            closeTable
            numErrors += 1
            if (numErrors >= MAX_NUM_HBASE_RETRIES) {
              log.warn(s"HBase mutation retry: ${numErrors}, mutation size = ${compactedQueue.size}", e)
              handleError(e)
              return
            }
          }
        } finally {
          compactionSemaphore.release(numFetchers)
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
