package twitter

import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession


/*
Class to count followers using a DataSet
 */

object RSdataset {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Argument check
    if (args.length != 3) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Triangle Count")

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._

    val max_filter = args(2).toInt

    val linkDataSet = sparkSession.read.csv(args(0)).toDF("follower", "followee")
                                                    .filter(entry => entry.getAs[String]("follower").toInt < max_filter && entry.getAs[String]("followee").toInt < max_filter)

    val path2 = linkDataSet.as("l1").join(linkDataSet.as("l2"), $"l1.followee" === $"l2.follower")
                                            .toDF("follower1", "followee1", "follower2", "followee2")
                                            .map(entry => (entry.getAs[String]("follower1"), entry.getAs[String]("followee2")))


    val triangleSet = path2.as("p2").join(linkDataSet.as("l1"), $"p2._2" === $"l1.follower")
                                      .toDF("follower1", "followee1", "follower2", "followee2")
                                      .filter(entry => entry.getAs[String]("follower1")==entry.getAs[String]("followee2"))

    println(triangleSet.count()/3)


  }
}
