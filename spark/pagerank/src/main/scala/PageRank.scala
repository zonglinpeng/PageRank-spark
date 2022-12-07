import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Using


object PageRank {
  def pageRank(
      sparkSession: SparkSession,
      verticesFilePath: String,
      edgesFilePath: String,
      dampingFactor: Double = 0.85,
      tolerance: Double = 1e-8
    ): RDD[(Long, Double)] = {
    val vertices = sparkSession.read.textFile(verticesFilePath).rdd.map(_.toLong)
    val edges = sparkSession.read.textFile(edgesFilePath).rdd.map({ 
      l =>
      val line = l.split("\\s+")
      val src = line(0).toLong
      val dst = line(1).toLong
      (src, dst)
    })
    var iterationTime = 0
    var maxRankOffet = Double.MaxValue
    val nilDegreeVertices = vertices.subtract(edges.map(_._1))
    val graph = edges.groupByKey().++(nilDegreeVertices.map(x => (x, Seq())))
    var ranks = vertices.map(x => (x, 1.0))
    val contributionBas = vertices.map(x => (x, 0.0))

    while (maxRankOffet >= tolerance) {
      iterationTime += 1
      val contributions = graph.join(ranks).values
        .flatMap {
          case (destinations, rank) =>
            val outDegree = destinations.size
            destinations.map(dst => (dst, rank / outDegree))
        }

      // https://neo4j.com/docs/graph-data-science/current/algorithms/page-rank/
      val nextRanks = (contributionBas.++(contributions))
        .reduceByKey(_ + _)
        .mapValues(1 - dampingFactor + dampingFactor * _) 

      maxRankOffet = ranks.join(nextRanks).values
        .map { 
          case (prevRank, currRank) => 
            (currRank - prevRank).abs 
        }
        .max()

      ranks = nextRanks
    }

    println(s"Iteration Time: $iterationTime")
    val correction = ranks.count() / ranks.map(_._2).sum()
    ranks.mapValues(_ * correction)
  }
  

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Read README for help")
      System.exit(1)
    }

    val verticesPath = args(0)
    val edgesPath = args(1)
    val topK = if (args.length > 2) args(2).toInt else 5

    Using(
      SparkSession
        .builder
        .appName("PageRank")
        .getOrCreate()
    ) { 
      spark =>
      spark.sparkContext.setLogLevel("ERROR")
      val startTimeMillis = System.currentTimeMillis()
      val ranks = pageRank(spark, verticesPath, edgesPath)
      val endTimeMillis = System.currentTimeMillis()
      val topKRanks = ranks.sortBy(_._2, ascending = false).take(topK)
      println(s"time taken: ${endTimeMillis - startTimeMillis}")
      topKRanks.foreach {
        case (vertexID, rank) => {
          println(s"paper id: ${vertexID}\tpage-rank: ${"%.5f".format(rank)}")
        }
      }
    }
  }
}
