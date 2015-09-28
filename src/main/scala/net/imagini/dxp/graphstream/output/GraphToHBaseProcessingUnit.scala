package net.imagini.dxp.graphstream.output

import java.io.IOException
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import kafka.message.MessageAndOffset
import net.imagini.dxp.common.BSPMessage
import org.apache.donut.{FetcherDelta, Fetcher, DonutAppTask}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 28/09/15.
 */
class GraphToHBaseProcessingUnit(config: Properties, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  private val log = LoggerFactory.getLogger(classOf[GraphToHBaseProcessingUnit])

  val MAX_MUTATION_SIZE = 5000  // TODO max mutation size config
  val MAX_MUTATION_TIME_MS = 1000L // TODO max mutation time config
  val MAX_NUM_HBASE_RETRIES = 5 // TODO max mutation num.retries config
  val tableNameAsString = config.getProperty("hbase.table")

  val hbaConf = HBaseConfiguration.create()
  hbaConf.addResource(config.getProperty("yarn1.site") + "/core-site.xml")
  hbaConf.addResource(config.getProperty("yarn1.site") + "/hdfs-site.xml")
  hbaConf.addResource(config.getProperty("yarn1.site") + "/yarn-site.xml")
  hbaConf.addResource(config.getProperty("hbase.site") + "/hbase-site.xml")

  private val mutationQueue = new LinkedBlockingQueue[Mutation](MAX_MUTATION_SIZE * 2)

  private val mutation = new java.util.LinkedList[Mutation]

  private var mutationCounter = 0L

  private var connection: Connection = null

  private var table: BufferedMutator = null

  override protected def awaitingTermination: Unit = {
    println(s"TOTAL MUTATIONS IN TABLE `${tableNameAsString}` = ${mutationCounter}")
  }

  override protected def onShutdown: Unit = {
    closeTable
    LoaderThread.interrupt
    mutationQueue.synchronized(mutationQueue.notify)
  }

  LoaderThread.start

  private object LoaderThread extends Thread {
    override def run: Unit = {
      while (!isInterrupted) {
        Thread.sleep(MAX_MUTATION_TIME_MS)
        mutationQueue.drainTo(mutation, MAX_MUTATION_SIZE)
        var numErrors = 0L
        while (mutation.size >0) {
          try {
            if (connection == null) {
              log.info("Opening HBase connection")
              connection = ConnectionFactory.createConnection(hbaConf)
              log.info(s"Opening HBase Buffered Mutator for table `${tableNameAsString}")
              table = connection.getBufferedMutator(TableName.valueOf(tableNameAsString))
            }
            log.debug(s"HBase mutation size = ${mutation.size}")
            //table.mutate(mutation)
            mutationCounter += mutation.size
            mutation.clear
          } catch {
            case e:IOException => {
              closeTable
              numErrors += 1
              if (numErrors >= MAX_NUM_HBASE_RETRIES) {
                log.warn(s"HBase mutation retry: ${numErrors}, mutation size = ${mutation.size}", e)
                handleError(e)
                return
              }
            }
          }
        }
      }
    }
  }

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "graphdelta" => new FetcherDelta(this, topic, partition, groupId) {
        override protected def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          val key = messageAndOffset.message.key
          val payload = messageAndOffset.message.payload
          var delete: Delete = null
          if (payload == null) {
            delete = new Delete(key.array, key.arrayOffset, key.remaining)
            delete.addFamily(Bytes.toBytes("N"))
          } else {
            val put = new Put(key.array, key.arrayOffset, key.remaining)
            val inputEdges = BSPMessage.decodePayload(payload.array, payload.arrayOffset)._2
            inputEdges.foreach { case (destVid, destEdge) => {
              if (destEdge.probability == 0) {
                if (delete == null) {
                  delete = new Delete(key.array, key.arrayOffset, key.remaining)
                }
                delete.addColumn(Bytes.toBytes("N"), destVid.bytes)
              } else {
                put.addColumn(Bytes.toBytes("N"), destVid.bytes, destEdge.ts, destEdge.bytes)
              }
            }}
            mutationQueue.offer(put)
          }
          if (delete != null) {
            mutationQueue.offer(delete)
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
