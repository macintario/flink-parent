package org.apache.flink.examples.java.clustering.util
/*
* Instituto Politécnico Nacional
* Centro de Investigación en computación
* Diplomado de Descubrimiento del Conocimiento
* Usando Herramientas de Big Data
*
* Alumno: Iván Gutiérrez Rodríguez
*
* Archivo: knn_files.scala
*
*  Descripción: Implemanta KNN  y usa como entrada dos Archivos
*   1: iris.csv        El dataset clásico de Iris https://raw.githubusercontent.com/uiuc-cse/data-fa14/gh-pages/data/iris.csv
*   2: validate.csv    Puntos de pruena a clasificar. Se hizo manual con valores en el rango de los datos.
* Está basado en el ejercicio de clase, pero se modificó para que manejara las cuatro dimensiones del archivo "iris.csv"
*
* */
import org.apache.commons.lang3.mutable.Mutable
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.common.operators.Order._
import org.apache.flink.api.java.DataSet





object knn_files {
  // Se modifica respecto al de clase para abarcar cuatro dimensiones y la categoría como último campo
  case class Point(w: Double, x: Double, y:Double, z:Double, label: String)
  case class NewPoint(label: String, distance: Double)
  // Se crea una nueva clase para mostrar los resultados, ya que sólo mostraba
  // la etiqueta asignada y la distancia, pero no mostraba las coordenadas del punto
  case class FinalPoint(label: String, distance: Double, w: Double, x: Double, y:Double, z:Double)
  //Se modificó para abarcar cuatro dimensiones
  case class PointValidate(w: Double, x: Double, y:Double, z:Double)

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    val k = 3
    // Se modifica para leer los datos de entrenamianto
    val pointsTrain: scala.DataSet[Point] =
      env.readCsvFile(
        "/git/macintario_at_gmail/Diplomado Big Data/Modulo 4/flinka/flink(1)/flink-master/flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/clustering/util/iris.csv",
        ignoreFirstLine = true)
    // Se modifica para leer los datos a clasificar
    val pointsClas : scala.DataSet[PointValidate] = env.readCsvFile(
      "/git/macintario_at_gmail/Diplomado Big Data/Modulo 4/flinka/flink(1)/flink-master/flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/clustering/util/validate.csv"
    )
    // Se modifica para no usar un "for" y usar directamente "map"
    val clasificados = pointsClas.collect().map(x => asignaEtiqueta(x, pointsTrain, k))
    //Muestra los resultados
    print("Parámetro k=")
    println(k)
    println("Resultados")
    println(clasificados)
    //clasificados.
  }
  // Se extrae el código del ejercicio de clase y se implementa como función
  // para usarla en el "map"
  def asignaEtiqueta  (pV: PointValidate, pointsTrain: scala.DataSet[Point], k:Int) ={
    val train = pointsTrain
    var puntofinal = train.map(x => NewPoint(x.label,
      math.sqrt(   //Se modifica la distancia para que lo haga en cuatro dimensiones
        (pV.x-x.x)*(pV.x-x.x)
          +(pV.y-x.y)*(pV.y-x.y)
          +(pV.w-x.w)*(pV.w-x.w)
          +(pV.z-x.z)*(pV.z-x.z)
      )))
      .sortPartition(1,order = ASCENDING)
      .first(k)
      .map(x  => NewPoint(x.label,1))
      .groupBy(0)
      .sum(1)
      .sortPartition(1,DESCENDING)
      .first(1)
//      .map(x=>Point(pV.w,pV.x,pV.y,pV.z,x.label))
      .map(x=>FinalPoint(x.label, x.distance,pV.w,pV.x,pV.y,pV.z))
    puntofinal.collect().toString()
  }

}

