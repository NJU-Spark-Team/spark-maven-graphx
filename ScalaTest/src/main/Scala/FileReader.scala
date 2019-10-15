import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object FileReader{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val path : String = "C:\\Users\\Disclover\\Desktop\\"

    val conf = new SparkConf().setAppName("MavenDependencyRelation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    println("LOAD BEGIN!")
    val ls : Long = System.currentTimeMillis()
    val graph : Graph[Int, Int] = GraphLoader.edgeListFile(sc, path + "mvn-edges.txt").cache()
    val outDegrees: Graph[Int, Int] = GraphLoader.edgeListFile(sc, path + "mvn-node-deg.txt").cache()
    var outs = mutable.Map[Int, Int]()
    outDegrees.edges.collect.foreach(e =>
      outs += (e.srcId.toInt -> e.dstId.toInt)
    )


    println("LOAD COMPLETE!")
    val le : Long = System.currentTimeMillis()
    println("Loading time : " + (le - ls) + " ms")
    println("-----------------------------------")

    println("COMPUTE BEGIN!")
    val out : FileWriter = new FileWriter(path + "mvn-res.txt")
    val source : BufferedSource =  Source.fromFile(path + "mvn-nodes.txt")
    for (line : String <- source.getLines()){
      val strs : Array[String] = line.split(",")
      val simList : List[(Int, Double)] = func(strs(0).toInt, graph, outs)
      var res : String = strs(0) + ":"
      simList.foreach(e => res += e._1 + "," + e._2.formatted("%.2f") + ";")
    }
    source.close()
    out.close()
    println("COMPUTE COMPLETE!")
    val ce : Long = System.currentTimeMillis()
    println("Computing time : " + (ce - le) + " ms")
    println("-----------------------------------")

    println("QUIT!")

  }

  def func(node: Int, graph: Graph[Int, Int], outs: mutable.Map[Int, Int]): List[(Int, Double)] = {
    //    node.jar所依赖的所有包
    var depList: List[Int] = List()
    graph.edges.collect.foreach(e =>
      if (e.srcId == node) {
        depList = depList :+ e.dstId.toInt
      })

    //    与node.jar有相同依赖包的包
    var map = mutable.Map[Int, Int]()
    graph.edges.collect.foreach(e =>
      if (depList.contains(e.dstId.toInt)) {
        val eid = e.srcId.toInt
        if (map.contains(eid)) {
          map += (eid -> (map(eid) + 1))
        } else {
          map += (eid -> 1)
        }
      })

    //    初始化存储其余几点二阶相似度的map
    var similarities = mutable.Map[Int, Double]()

    var res: List[(Int, Double)] = List()
    val outOfNode: Int = outs(node)

    map.foreach(e =>
      if (e._1 != node) {
        val out: Int = outs(e._1)
        val common: Int = 2 * e._2
        val curSimilarity: Double = common.toDouble / (outOfNode + out).toDouble
        similarities += (e._1 -> curSimilarity)
      })

    //    排序（根据key降序排序）
    val sorts = similarities.toList.sortBy(-_._2)
    if (sorts.size >= 5) {
      res = sorts.take(5)
    } else {
      res = sorts
    }

    println(node + "-----" + res)

    res
  }
}
