package net.imagini.dxp.graphstream.output

import java.util.Properties

import kafka.message.MessageAndOffset
import org.apache.donut.{FetcherDelta, Fetcher, DonutAppTask}

/**
 * Created by mharis on 28/09/15.
 */
class GraphToHBaseProcessingUnit(config: Properties, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  //TODO open hbase client - requires config to contain hbase connection parameters

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
