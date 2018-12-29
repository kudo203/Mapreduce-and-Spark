package twitter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager


/*
Class to count followers
 */
object RSrdd {
  
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

    // New Edges skipping
    val path2 = reverseEdgesRDD.join(forwardEdgesRDD).map(joinedLink => (joinedLink._2._1, joinedLink._2._2))

    val allTriangles = path2.join(reverseEdgesRDD).filter(joinedTriangle => joinedTriangle._2._1 == joinedTriangle._2._2)

    println(allTriangles.count()/3)
  }
}