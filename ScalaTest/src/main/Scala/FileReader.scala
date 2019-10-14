import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}
import spire.std.long

import scala.io.Source
import scala.util.control.Breaks

object FileReader{
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("MavenDependencyRelation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    println("LOAD BEGIN!")
    val s : Long = System.currentTimeMillis()
    val graph : Graph[Int, Int] = GraphLoader.edgeListFile(sc, "C:\\Users\\Disclover\\Desktop\\mvn-edges.txt").cache()

//    graph.edges.collect.foreach(println(_))

    println("LOAD COMPLETE!")
    println("Total time : " + (System.currentTimeMillis() - s))

    val node : Int = args(0).toInt

  }
}
