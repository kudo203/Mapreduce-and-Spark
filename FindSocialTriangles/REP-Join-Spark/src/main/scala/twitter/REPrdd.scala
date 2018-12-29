package twitter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import scala.collection.mutable


/*
Perform Replicated Join using RDD
 */
object REPrdd {
  
  def main(args: Array[String]): Unit = {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Checking Argument length
    if (args.length != 3) {
      logger.error("Usage:\nwc.TwitterFollowersCountMain <input nodes dir> <input edges dir> <output dir> <max>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Twitter Triangle Count")

    val sc = new SparkContext(conf)

    val max_filter = args(2).toInt

    // Creating Pair RDD from edges file
    val fileRDD = sc.textFile(args(0))

    val followSplitRDD = fileRDD.map(line => {
      val splitLine = line.split(",")
      (splitLine(0), splitLine(1))
    })
    // Filtered original edges
    val forwardEdgesRDD = followSplitRDD.filter(x => x._1.toInt < max_filter && x._2.toInt < max_filter)

    // Filtered reversed edges
    val reverseEdgesRDD = forwardEdgesRDD.map(entry => (entry._2, entry._1))

    // set aggregate functional arguments
    val addToSet = (s: mutable.MutableList[String], v: String) => s += v
    val mergeSet = (l1: mutable.MutableList[String], l2: mutable.MutableList[String]) => l1 ++= l2

    // preparing broadcast
    val broadcastRDD = forwardEdgesRDD.aggregateByKey(mutable.MutableList.empty[String])(addToSet, mergeSet)
    val broadcast = sc.broadcast(broadcastRDD.collectAsMap())


    // Generates inverted paths
    val reversedTwoPath = reverseEdgesRDD.mapPartitions(iter => {
      iter.flatMap {
        case (k, v) =>{
          broadcast.value.get(k) match {
            case None => Seq.empty[(String, String)]
            case Some(neighbors) => {
              var paths2 = Seq.empty[(String, String)]
              for(x <- neighbors){
                paths2 = paths2 :+ (x, v)
              }
              paths2
            }
          }
        }
      }
    }, preservesPartitioning = true)

    // Generates Triangles
    val triangle = reversedTwoPath.mapPartitions(iter => {
      iter.flatMap {
        case (k, v1) =>{
          broadcast.value.get(k) match {
            case None => Seq.empty[(String, String)]
            case Some(v2) =>  if (v2.contains(v1)) Seq((v1, v1)) else Seq.empty[(String, String)]
          }
        }
      }
    })

    println(triangle.count()/3)

  }
}