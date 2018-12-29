package twitter

import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


/*
Class to count followers using a DataSet
 */

object DSET {

  case class LinkEdges(follower: Int, followee: Int)

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Argument check
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Count Followers").setMaster("local[2]")

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._

    val linkSchemaString = "follower,followee"
    val linkFields = linkSchemaString.split(",")
                                     .map(name => StructField(name, IntegerType))
    val linkDataSet = sparkSession.read.schema(StructType(linkFields)).csv(args(0)).as[LinkEdges]

    val followerCount = linkDataSet.map(link => (link.followee, 1))
    val outputDataset = followerCount.groupBy("_1").count()

    outputDataset.write.csv(args(1))

    logger.info("Explain Info: \n" + outputDataset.explain(true))
  }
}