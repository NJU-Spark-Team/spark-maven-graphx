import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

object FileReader{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("MavenDependencyRelation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    println("LOAD BEGIN!")
    val s : Long = System.currentTimeMillis()
    val graph : Graph[Int, Int] = GraphLoader.edgeListFile(sc, "C:\\Users\\Disclover\\Desktop\\testdata.txt").cache()

    println("LOAD COMPLETE!")
    println("Loading time : " + (System.currentTimeMillis() - s) + " ms")

    for (line : String <- Source.fromFile("").getLines()){
      val strs : Array[String] = line.split(",")
      val simList : List[(Int, Double)] = func(strs(0).toInt, graph)
      var res : String = strs(0) + ":"
      simList.foreach(e => res += e._1 + "," + e._2 + ";")
    }



  }

  def func(node : Int, graph : Graph[Int, Int]) : List[(Int, Double)] = {
//    node.jar所依赖的所有包
    var depList : List[Int] = List()
    graph.edges.collect.foreach(e =>
      if (e.srcId == node){
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

    var similarity : Double = 0.0
    var res : List[(Int, Double)] = List()
    val outOfNode : Int = depList.size

    map.foreach(e =>
      if (e._1 != node){
        var out : Int = 0
        /**
         * TODO
         */
        //        graph.edges.collect.foreach(f =>
//          if (f.srcId == e._1){
//            out += 1
//          })
        val common : Int = map(node) + e._2
        val curSimilarity : Double = (common * 2) / (outOfNode + out)
        if (curSimilarity > similarity){
          res = List((e._1, curSimilarity))
          similarity = curSimilarity
        }
        else if (curSimilarity == similarity){
          res = res :+ (e._1, curSimilarity)
        }
      })

    res
  }
}
