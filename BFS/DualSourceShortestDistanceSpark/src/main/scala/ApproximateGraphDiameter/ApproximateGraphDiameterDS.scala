package ApproximateGraphDiameter

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,
  DoubleType, LongType, ArrayType}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer,
  UserDefinedAggregateFunction}
import org.apache.spark.sql.Row

import scala.collection.mutable.WrappedArray
import scala.collection.mutable.{ListBuffer,ArrayBuffer}

import scala.collection.mutable.ListBuffer


object ApproximateGraphDiameterDS {

  case class Dist(v: Int, dis1: Long, dis2: Long)

  case class Adjacency(v: Int, adj: Seq[Int])


  def extractVertices(adjacency: Adjacency, prevDis1: Long, prevDis2: Long): Seq[Dist] = {
    var ans = Seq.empty[Dist]
    var newDis1 = prevDis1
    var newDis2 = prevDis2

    if(newDis1 != Long.MaxValue)
      newDis1 = newDis1 + 1

    if(newDis2 != Long.MaxValue)
      newDis2 = newDis2 + 1

    ans = ans :+ Dist(adjacency.v, prevDis1, prevDis2)

    for(neighbor <- adjacency.adj){
      ans = ans :+ Dist(neighbor, newDis1, newDis2)
    }

    ans
  }

  def deserialize(link: String): Adjacency = {
    var newLink = link.substring(1, link.length-1)
    var split = newLink.split(":")
    var neighbors = Seq.empty[Int]
    for(neighbor <- split(1).split(",")){
      if(neighbor.length != 0){
        neighbors = neighbors :+ neighbor.toInt
      }
    }
    Adjacency(split(0).toInt, neighbors)
  }

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 4) {
      logger.error("Usage:\nPageRank.ApproximateGraphDiameterDS <k1> <k2> <input> <output> ")
      System.exit(1)
    }
    val k1 = args(0).toInt
    val k2 = args(1).toInt

    val conf = new SparkConf().setAppName("Approximate Graph Diameter").setMaster("local[2]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._

    var adjList: Seq[Adjacency] = Seq.empty[Adjacency]

    var readAdj = sparkSession.read.text(args(2))

    var finalAdj = readAdj.map(x => deserialize(x.toString()))

    finalAdj.persist()

    var distances = finalAdj.map{x =>
      if(x.v == k1) Dist(x.v, 0, Long.MaxValue)
      else if(x.v == k2) Dist(x.v, Long.MaxValue, 0)
      else Dist(x.v, Long.MaxValue, Long.MaxValue)
    }

    for(x <- 1 to 6){
      var temp = finalAdj
        .joinWith(distances, finalAdj("v") ===  distances("v"))
        .flatMap(x => extractVertices(x._1, x._2.dis1, x._2.dis2))


      distances = temp.groupBy(temp("v")).agg(
        min($"dis1").alias("dis1"),
        min($"dis2").alias("dis2")
      ).map(x => Dist(x.getInt(0), x.getLong(1), x.getLong(2)))
    }


    distances.show()
  }
}
