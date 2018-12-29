package PageRank

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

import util.control.Breaks._
import scala.collection.mutable.ListBuffer

object RankPageRDD {

  def main(args: Array[String]): Unit ={
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if(args.length != 1){
      logger.error("Usage:\nPageRank.RankPageRDD <k>")
      System.exit(1)
    }
    val k : Int = args(0).toInt

    val graph = new ListBuffer[(Int, Int)]()
    val rank = new ListBuffer[(Int, Double)]

    val kSquared = k*k
    val inverseKSquared = 1/kSquared.toDouble

    // Custom edge list generation
    for (index <- 0 to kSquared){
      breakable{
        if(index == 0){
          rank.append((0, 0.0))
          break()
        }
        rank.append((index, inverseKSquared))
        if(index % k == 0){
          graph.append((index, 0))
        }
        else{
          graph.append((index, index+1))
        }
      }
    }


    val conf = new SparkConf().setAppName("Page Rank Using RDD").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Convert listbuffer to an RDD
    val graphRdd = sc.parallelize(graph)
    var graphPairRDD = graphRdd.map(x => (x._1, x._2)).partitionBy(new HashPartitioner(8))
    graphPairRDD.persist()

    val rankRdd = sc.parallelize(rank)
    var rankPairRDD = rankRdd.map(x => (x._1, x._2))

    // Page rank loop
    for(iter <- 1 to k){
      var temp = graphPairRDD.join(rankPairRDD).map(x => (x._2._1, x._2._2))
      var temp2 = temp.reduceByKey(_ + _)
      var delta = temp2.lookup(0)(0)
      var danglingPr = delta/kSquared
      rankPairRDD = temp2.map(x => if(x._1 != 0) (x._1, danglingPr + x._2) else (x._1, 0.0))

      for(i <- 0 until k){
        rankPairRDD = rankPairRDD.union(sc.parallelize(Seq((i*k + 1, delta/kSquared))))
      }
    }

    var topK = rankPairRDD.takeOrdered(k*k)(Ordering[Double].reverse.on(x=>x._2))
    topK.foreach(println)
  }
}
