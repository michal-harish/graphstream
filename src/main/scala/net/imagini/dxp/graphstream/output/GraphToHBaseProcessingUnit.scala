package net.imagini.dxp.graphstream.output

import java.util.Properties

import kafka.message.MessageAndOffset
import org.apache.donut.{FetcherDelta, Fetcher, DonutAppTask}
import org.apache.hadoop.hbase.HBaseConfiguration

/**
 * Created by mharis on 28/09/15.
 */
class GraphToHBaseProcessingUnit(config: Properties, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  val hbaConf = HBaseConfiguration.create()
  hbaConf.addResource(config.get("yarn1.site") + "/core-site.xml")
  hbaConf.addResource(config.get("yarn1.site") + "/hdfs-site.xml")
  hbaConf.addResource(config.get("yarn1.site") + "/yarn-site.xml")
  hbaConf.addResource(config.get("hbase.site") + "/hbase-site.xml")

  //TODO open hbase client

  override protected def onShutdown: Unit = {
    //TODO disconnect hbase client if any
  }

  override protected def awaitingTermination: Unit = {
    //TODO print processed stats
  }

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "graphstate" => new FetcherDelta(this, topic, partition, groupId) {
        override protected def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          /*
           * TODO load all available messages and create and submit hbase bulk load operation - so this job must run as 'hbase' user
           * This requires the FetcherDelta to expose 'caught-up' flag OR we have a minimum time/num.messages to load
           */
        }
      }
    }
  }

}
