package ApproximateGraphDiameter

import org.apache.log4j.LogManager
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object ApproximateGraphDiameterRDD {

  def extractVertices(adjacencyList: String, prevDis1: Long, prevDis2: Long): TraversableOnce[(String, (Long, Long))] = {
    var ans = Seq.empty[(String, (Long, Long))]
    var newDis1 = prevDis1
    if (newDis1 != Long.MaxValue) {
      newDis1 = newDis1 + 1
    }
    var newDis2 = prevDis2
    if(newDis2 != Long.MaxValue){
      newDis2 = newDis2 + 1
    }

    val split = adjacencyList.split(",")
    for (x <- 0 until split.length) {
      if(x == 0){
        ans = ans :+ (split(x), (prevDis1, prevDis2))
      }
      else{
        ans = ans :+ (split(x), (newDis1, newDis2))
      }
    }
    ans
  }

  def comparision(x: (Long, Long), y: (Long, Long)) : (Long, Long) = {
    (Math.min(x._1, y._1), Math.min(x._2, y._2))
  }


  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 4) {
      logger.error("Usage:\nPageRank.ApproximateGraphDiameterRDD <k1> <k2> <input> <output> ")
      System.exit(1)
    }
    val k1 = args(0)
    val k2 = args(1)

    val conf = new SparkConf().setAppName("Approximate Graph Diameter").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Creating Pair RDD from edges file
    val fileRDD = sc.textFile(args(2))

    val aRdd = fileRDD.map { line => {
      val split = line.split(":")
      (split(0), split(0) + "," + split(1))
    }}.partitionBy(new HashPartitioner(8))

    aRdd.persist()

    var distance = aRdd.mapValues{x =>
      if(x.split(",")(0) == k1) (0, Long.MaxValue)
      else if(x.split(",")(0) == k2) (Long.MaxValue, 0)
      else (Long.MaxValue, Long.MaxValue)
    }

    for(m <- 1 to 10){
      distance = aRdd
        .join(distance)
        .flatMap(x => extractVertices(x._2._1, x._2._2._1.asInstanceOf[Number].longValue, x._2._2._2.asInstanceOf[Number].longValue))
        .reduceByKey((x,y) => comparision(x,y)).mapValues(x => (x._1, x._2))
    }


    distance.collect().foreach(println)
  }
}

