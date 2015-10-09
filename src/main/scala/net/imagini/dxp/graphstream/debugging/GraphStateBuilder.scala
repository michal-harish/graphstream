package net.imagini.dxp.graphstream.debugging

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import net.imagini.dxp.common.BSPMessage
import org.apache.donut._
import org.apache.donut.memstore.{MemStoreLogMap, MemStoreMemDb}

/**
 * Created by mharis on 08/10/15.
 */
class GraphStateBuilder(config: Properties) extends DonutApp[GraphStateBuilderProcessor]({

  config.setProperty("group.id", "DebugGraphStateBuilder")
  config.setProperty("topics", "graphstate")
  config.setProperty("direct.memory.mb", "5000")
  config.setProperty("task.overhead.memory.mb", "2048") //  +2g heap overhead per task
  config.setProperty("yarn1.jvm.args", "-XX:+UseSerialGC -XX:NewRatio=3 -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
  config.setProperty("yarn1.restart.enabled", "false")
  config.setProperty("yarn1.restart.failed.retries", "3")
  config
})

class GraphStateBuilderProcessor(config: Properties, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  val maxStateSizeMb = config.getProperty("direct.memory.mb").toInt / totalLogicalPartitions

  println(s"MAX STATE SIZE IN MB = ${maxStateSizeMb}")

  val altstore = new MemStoreLogMap(maxStateSizeMb, 16, 4 * 65535)
  val altmap = altstore.map
  var ts = System.currentTimeMillis
  val stateIn = new AtomicLong(0)

  override protected def awaitingTermination: Unit = {
    val t = (System.currentTimeMillis - ts)
    val s = stateIn.get * 1000 / t
    ts = System.currentTimeMillis
    stateIn.set(0)
    println(s"graphstate (${s} / sec)")
    altmap.printStats
    println(s"")
  }

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "graphstate" => new FetcherBootstrap(this, topic, partition, groupId) {
        def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          stateIn.incrementAndGet
          buildState(messageAndOffset.message.key, messageAndOffset.message.payload)
        }
      }
    }
  }

  override protected def onShutdown: Unit = {}

  def buildState(msgKey: ByteBuffer, payload: ByteBuffer) = {
    val vid = BSPMessage.decodeKey(msgKey)
    BSPMessage.encodeKey(vid) match {
      case invalidKey if (!invalidKey.equals(msgKey)) => println(s"Invalid Key ${invalidKey}")
      case validKey => BSPMessage.decodePayload(payload) match {
        case null => List()
        case (i, edges) => {
          BSPMessage.encodePayload((i, edges)) match {
            case invalidPayload if (!invalidPayload.equals(payload)) => println(s"Invalid Oayload ${invalidPayload}")
            case validPayload => {
                altstore.put(validKey, validPayload)
            }
          }
        }
      }
    }
  }
}