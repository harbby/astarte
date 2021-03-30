package compare.spark.pagerank

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageRank {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkPageRank")
      .master("local[2]")
      .getOrCreate()

    val iters = 3

    val lines: RDD[(Integer, Array[Int])] = spark.sparkContext.makeRDD(Array("/data/data/ClueWeb09_WG_50m.graph-txt"), 1)
      .flatMap(file => new ClueWeb09IteratorReader(file))

    val links = lines.partitionBy(new HashPartitioner(2)).cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs: RDD[(Integer, Double)] = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (scala.Int.box(url), rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.mapPartitions(_.take(10)).collect()
    output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))

    spark.stop()
  }
}
