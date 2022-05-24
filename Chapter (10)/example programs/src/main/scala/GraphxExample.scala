import org.apache.spark._
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphxExample {
  def main(args: Array[String]): Unit = {
    /** Create spark session */
    val sparkSession = SparkSession.builder().appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val users = (sc.textFile("resources/example-data/graphx/celebrities.txt")
      .map(line => line.split(",")).map(parts => (parts.head.toLong, parts.tail)))
    println(s"users " + users)
    val fanGraph = GraphLoader.edgeListFile(sc, "resources/example-data/graphx/fans.txt")

    val graph = fanGraph.outerJoinVertices(users) {
      case (uid, degree, Some(attrList)) => attrList

      case (uid, degree, None) => Array.empty[String]
    }


  }
}
