package net.imagini.dxp.common

import java.util.UUID

import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 30/09/15.
 */
class IdSpaceTest  extends FlatSpec with Matchers {

  behavior of "UUID IdSpace"

  it should "partition symmetrically wotj 1000000 random samples" in {
    val vids: Seq[Vid] = (1 to 1000).map(i => Vid(UUID.randomUUID.toString))
    testDistribution(numPartitions = 32, maxStDev = 10.0, vids)

  }

  behavior of "LongPositive IdSpace"

  it should "partition symmetrically" in {
    val vids = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/appnexus/sample_ids"))
      .getLines.map(id => {
      val vid = Vid("a", id)
      id should be(vid.asString)
      vid
    }).toSeq
    testDistribution(numPartitions = 32, maxStDev = 6.0, vids)
  }

  behavior of "LongHash IdSpace"

  it should "partition symmetrically" in {
    val vids = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/rocketfuel/sample_ids"))
      .getLines.map(id => {
      val vid = Vid("test3", id)
      id should be(vid.asString)
      vid
    }).toSeq
    testDistribution(numPartitions = 32, maxStDev = 14.0, vids)
  }

  private def testDistribution(numPartitions: Int, maxStDev: Double, vids: Seq[Vid]) = {
    val hist = new scala.collection.mutable.HashMap[Int, Int]()
    var n = 0
    vids.foreach { vid =>
      val partition = Vid.getPartition(numPartitions, vid)
      n +=1
      if (hist.contains(partition)) hist += (partition -> (hist(partition) + 1)) else hist += (partition -> 1)
    }

    println(s"TESTING DISTRIBUTION OF ${n} VIDs ACROSS ${numPartitions} PARTITIONS")
    hist should have size numPartitions
    val expMean = n.toDouble / numPartitions
    println(s"EXPECTED MEAN COUNT PER PARTITON = ${expMean}")
    println(s"BEST POSSIBLE DISTRIBUTION STDEV = 0")

    println(s"ACTUAL PARTITION COUNT = ${hist.size}")
    val mean = hist.map(_._2.toDouble).sum / hist.size
    val min =  hist.map(_._2.toDouble).min
    val max =  hist.map(_._2.toDouble).max
    val stddev = math.sqrt(hist.map(a => math.pow(a._2, 2)).sum / hist.size - math.pow(mean, 2))
    println(s"ACTUAL MEAN COUNT PER PARTITION = ${mean}")
    println(s"ACTUAL DISTRIBUTION RANGE= [${min},${max}]")
    println(s"ACTUAL DISTRIBUTION STDEV = ±${stddev}")

    mean should be(expMean)
    stddev should be <= (maxStDev)

  }


}
