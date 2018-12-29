package PageRank

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object RankPageDS {

  case class Rank(v1: Int, pr: Double)
  def main(args: Array[String]): Unit = {
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

    // edge list generation
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

    val conf = new SparkConf().setAppName("Page Rank Using Dataset").setMaster("local[2]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._

    // Convert listbuffer to a dataset
    val graphDataSet = sparkSession.createDataset(graph)
    graphDataSet.persist()

    var rankDataSet = sparkSession.createDataset(rank)

    // Page rank loop
    for(iter <- 1 to k){
      var temp = graphDataSet.joinWith(rankDataSet, graphDataSet("_1") === rankDataSet("_1"))

      var temp2 = temp.groupBy(temp("_1._2")).agg(sum("_2._2"))

      var danglingPr = temp2.where(temp2("_2") === 0).collect()(0).getDouble(1)

      danglingPr = danglingPr/kSquared


      rankDataSet = temp2.map(x => if(x.getInt(0) == 0) (x.getInt(0), x.getDouble(1)) else (x.getInt(0), x.getDouble(1) + danglingPr))

      var startingNodes =  new ListBuffer[(Int, Double)]()

      for(i <- 0 until k){
        startingNodes.append((i*k + 1, danglingPr))
      }
      rankDataSet = rankDataSet.union(sparkSession.createDataset(startingNodes))
    }
    rankDataSet.show()
  }
}
