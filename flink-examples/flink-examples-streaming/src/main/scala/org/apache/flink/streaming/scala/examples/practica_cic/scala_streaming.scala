package org.apache.flink.streaming.scala.examples.practica_cic

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time



object scala_streaming {

  def main(args: Array[String]) {

    var hostname: String = "localhost"
    var port: Int = 0
    try{
      val params = ParameterTool.fromArgs(args)
      hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
      port = params.getInt("port")

    } //try
    catch{
      case e: Exception => {
        System.err.println("No port specified. Please Run 'scala_streaming " +
          "--hostname <hostname> --port <port>, where hostname (localhost by default) and port 9999")
        return
      }
    }//catch
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream(hostname,port,delimiter = '\n')
    val wc = text
      .flatMap(w => w.split("(?:^|(?<=\\s))(?=(\\S+\\s+\\S+)(?=\\s|$))/g"))
//      .flatMap(w => w.split("(\\S+\\s+\\S+)/gm"))
    println(wc)
val windowCounts = wc
      .map(w => WordWithCount(w,1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")

    windowCounts.print().setParallelism(1)
    print("--")
    env.execute("Streaming !!!")

  }//main
  case class WordWithCount(word:String, count: Long)
} //object scala_streaming
