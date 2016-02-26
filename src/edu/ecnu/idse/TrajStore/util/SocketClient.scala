package edu.ecnu.idse.TrajStore.util

import java.io.PrintWriter
import java.net.ServerSocket

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zzg on 15-11-30.
  */
object SocketClient {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[3]")

    conf.set("spark.serializer", "org,apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LongWritable], classOf[Text]))
    //conf.set("spark.kryoserializer.classToRegister","org.apache.hadoop.io.LongWritable,org.apache.hadoop.io.Text,org.apache.hadoop.io.LongWritable")
    val sc = new SparkContext(conf)

    //  val records = sc.newAPIHadoopFile[classOf[TextInputFormat[Text,Text]],classOf[Text],classOf[Text]]("hdfs://localhost:9000/user/zzg/Info-00-sortByTime")
//    sc.textFile("hdfs://localhost:9000/user/zzg/Info-00-sortByTime")
    val re = sc.hadoopFile("hdfs://localhost:9000/user/zzg/Info-00-sortByTime",
      classOf[SequenceFileInputFormat[LongWritable, Text]],
      classOf[LongWritable],
      classOf[Text]
    )
      .groupByKey(13)
      .sortByKey(true)
      .collect()

    var i = 0
    val listener = new ServerSocket(19999)
    val socket = listener.accept()
    new Thread() {
      override def run = {
        println("获得端口的链接从: " + socket.getInetAddress)
        val out = new PrintWriter(socket.getOutputStream(), true)
        while (i < re.length) {
          Thread.sleep(1000)
          // 当该端口接受请求时，随机获取某行数据发送给对方
          println("!!!!!!!!!!!!!--------------" + i + "--------------!!!!!!!!!!!!!!!!!")
          val key = re.apply(i)._1
    //      re.apply(i)._2.foreach(x => println(key + "\t" + x))
          re.apply(i)._2.foreach(x => out.println(key + "\t" + x))
          out.flush()

          i = i + 1
        }
        socket.close()
      }
    }.start()
  }
}
