package org.apache.flink.examples.java.clustering.util

import org.apache.commons.lang3.mutable.Mutable
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.common.operators.Order._



//TODO: Adaptarlo para archivos en lugar de valores fijos

object knn_files {
  case class Point(w: Double, x: Double, y:Double, z:Double, label: String)
  case class NewPoint(label: String, distance: Double)
  case class PointValidate(w: Double, x: Double, y:Double, z:Double)

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    val k = 6
    val pointsTrain: scala.DataSet[Point] =
      env.readCsvFile(
        "/git/macintario_at_gmail/Diplomado Big Data/Modulo 4/flinka/flink(1)/flink-master/flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/clustering/util/iris.csv",
        ignoreFirstLine = true)

    val pointsClas : scala.DataSet[PointValidate] = env.readCsvFile(
      "/git/macintario_at_gmail/Diplomado Big Data/Modulo 4/flinka/flink(1)/flink-master/flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/clustering/util/validate.csv"
    )

    val puntos = pointsClas.collect()
    for (dot <- puntos){
      val train = pointsTrain
      val puntofinal = train.map(x => NewPoint(x.label,
        math.sqrt(
            (dot.x-x.x)*(dot.x-x.x)
              +(dot.y-x.y)*(dot.y-x.y)
              +(dot.w-x.w)*(dot.w-x.w)
              +(dot.z-x.z)*(dot.z-x.z)
          )))
        .sortPartition(1,order = ASCENDING)
        .first(k)
        .map(x  => NewPoint(x.label,1))
        .groupBy(0)
        .sum(1)
        .sortPartition(1,DESCENDING)
        .first(1)

    }
    println(salida.length)
  }

}
