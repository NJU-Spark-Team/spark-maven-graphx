import java.io.FileWriter
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object GraphX {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val path: String = "src/main/resources/"

    val proc1 : Process = Runtime.getRuntime.exec("python " + path + "data_process.py pre-process " + path + "pkg-edges.csv")
    proc1.waitFor()
    val proc2 : Process = Runtime.getRuntime.exec("python " + path + "data_process.py translate " + path + "mvn-res.txt")
    proc2.waitFor()


    val conf = new SparkConf().setAppName("MavenDependencyRelation").setMaster("spark://120.77.42.11:7077")
    val sc = new SparkContext(conf)

    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, path + "mvn-edges.txt").cache()
    val outDegrees: Graph[Int, Int] = GraphLoader.edgeListFile(sc, path + "mvn-node-deg.txt").cache()
    var outs = mutable.Map[Int, Int]()
    outDegrees.edges.collect.foreach(e =>
      outs += (e.srcId.toInt -> e.dstId.toInt)
    )

    //    线程并发
    val threadPool: ExecutorService = Executors.newFixedThreadPool(10)

    val out: FileWriter = new FileWriter(path + "mvn-res3.txt")
    val source: BufferedSource = Source.fromFile(path + "mvn-nodes.txt")
    for (line: String <- source.getLines()) {
      val strs: Array[String] = line.split(",")
      threadPool.execute(new SingleThread(strs(0).toInt, graph, outs, out))
    }
    source.close()

  }

  def func(node: Int, graph: Graph[Int, Int], outs: mutable.Map[Int, Int]): List[(Int, Double)] = {
    // node.jar所依赖的所有包
    var depList: List[Int] = List()
    graph.edges.collect.foreach(e =>
      if (e.srcId == node) {
        depList = depList :+ e.dstId.toInt
      })

    // 与node.jar有相同依赖包的包
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

    // 初始化存储其余几点二阶相似度的map
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

    // 排序（根据key降序排序）
    val sorts = similarities.toList.sortBy(-_._2)
    if (sorts.size >= 5) {
      res = sorts.take(5)
    } else {
      res = sorts
    }

    res
  }

  class SingleThread(node: Int, graph: Graph[Int, Int], outs: mutable.Map[Int, Int], out: FileWriter) extends Runnable {
    override def run() {
      val simList: List[(Int, Double)] = func(node, graph, outs)
      var res: String = node + ":"
      simList.foreach(e => res += e._1 + "," + e._2.formatted("%.2f") + ";")
      out.write(res + "\n")
    }
  }

}
