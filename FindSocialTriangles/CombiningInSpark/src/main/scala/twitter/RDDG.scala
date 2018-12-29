package twitter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager



/*
Class to count followers using groupByKey
 */
object RDDG {
  
  def main(args: Array[String]) {
        val logger: org.apache.log4j.Logger = LogManager.getRootLogger

        // Argument check
        if (args.length != 2) {
          logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
          System.exit(1)
        }
        val conf = new SparkConf().setAppName("Word Count").setMaster("local[2]")
        val sc = new SparkContext(conf)

        // Creates an RDD
        val textFile = sc.textFile(args(0))

        val counts = textFile.map(line => line.split(",")(1))
                     .map(id => (id, 1))
                     .groupByKey()
                     .mapValues(_.size)
        counts.saveAsTextFile(args(1))

        logger.info("Lineage information: \n" + counts.toDebugString)
  }
}