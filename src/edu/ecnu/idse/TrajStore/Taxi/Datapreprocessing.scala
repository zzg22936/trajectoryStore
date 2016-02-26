package edu.ecnu.idse.TrajStore.Taxi

import java.text.SimpleDateFormat

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zzg on 15-11-19.
  */
object Datapreprocessing {
  def main(args: Array[String]): Unit = {

    /*val data = sc.textFile("/home/zzg/test1").flatMap(x=>{
      val tmp = x.s
    })*/

    staticAttrs
    //20131001000159
    //20131001000159,001140,116.406975,39.988724,45.000000,264.000000,1,1,0,20131001000159
   // getAllCars(args)
    //  mapCars(args)
    //   recordSortByTime
 //   secondSortTest
  }

  def secondSortTest(): Unit ={
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val result = sc.textFile("/home/zzg/test1").map(x=> x.split("\t"))
                    .map(x=>(x(0),x(1))).groupByKey()
    .sortByKey(true).map(x=>(x._1,x._2.toList.sortWith(_>_))).flatMap(x=>{
      val len = x._2.length
      val array = new Array[(Text,Text)](len)
      for(i<-0 until len){
        val key = new Text()
        val value = new Text()
        key.set(x._1)
        value.set(x._2(i))
        array(i) = (key,value)
      }
      array
    }).saveAsHadoopFile("/home/zzg/r",classOf[Text],classOf[Text],
      classOf[TextOutputFormat[Text,Text]])

/*
    val result = sc.textFile("/home/zzg/test1").map(x=> x.split("\t"))
      .map(x=>(x(0),x(1))).groupByKey()
      .sortByKey(true).map(x=>(x._1,x._2.toList.sortWith(_>_))).flatMap(x=>{
      val len = x._2.length
      val array = new Array[(String,String)](len)
      for(i<-0 until len){

        array(i) = (x._1,x._2(i))
      }
      array
    }).saveAsHadoopFile("/home/zzg/r",classOf[Text],classOf[Text],
      classOf[TextOutputFormat[Text,Text]])

    result.foreach(x=>{
      for(i<-0 until x._2.length){
        println(x._1 +"\t"+x._2(i))
      }
    })*/
  }

////183476:4430 hdfs://localhost:9000/user/zzg/Info-

  def recordSortByTime(): Unit ={
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[3]")
  conf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LongWritable],classOf[Text]))
    val sc = new SparkContext(conf)
    val carMap = sc.textFile("/home/zzg/id-map").map(x=>{
      val tmp =x.split("\t")
      (tmp.apply(0),tmp.apply(1).toInt)
    }).collectAsMap()
   /* carMap.foreach(e=>{
      println(e._1+":"+e._2)
    })
   println("carMap.get(\"183476\")"+carMap.get("183476"))*/
    val sd = new SimpleDateFormat("yyyyMMddHHmmss")
    val beginTime = sd.parse("20131001000000").getTime
    val endTime = sd.parse("20131001003459").getTime
    println(endTime - beginTime)
    val broadcastVal = sc.broadcast(beginTime)
    val broadCars = sc.broadcast(carMap)
//    val broadCastSD = sc.broadcast(sd)
   val records = sc.textFile("hdfs://localhost:9000/user/zzg/Info-00")
    .map(x=>{
      val tmp = x.split(",").toSeq
     // println(tmp.apply(0))
     val localSD = new SimpleDateFormat("yyyyMMddHHmmss")
      val time1 = new LongWritable(0)
      val time2 = new LongWritable(0)
      val value = new Text()
      try{
        val tNow = localSD.parse(tmp.apply(0)).getTime
        time1.set((tNow - broadcastVal.value)/1000)
        val tEnd = localSD.parse(tmp.apply(9)).getTime
        time2.set((tEnd -broadcastVal.value)/1000)
      }catch{
        case e: NumberFormatException =>{
          println(x+tmp.apply(0)+"!!!!!!")
        }
      }

      val cid = broadCars.value.get(tmp.apply(1)).get

      value.set(cid+","+tmp.apply(2)+","+tmp.apply(3)+","+tmp.apply(4)+","+tmp.apply(5)+","+tmp.apply(6)+","+tmp.apply(7)+","+tmp.apply(8) +","+time2)
     // val value = cid+","+tmp.apply(2)+","+tmp.apply(3)+","+tmp.apply(4)+","+tmp.apply(5)+","+tmp.apply(6)+","+tmp.apply(7)+","+tmp.apply(8) +","+time2

     (time1, value)
    }).groupByKey(13)
     .sortByKey(true)
     .flatMapValues(x=>x.toList)
    .saveAsHadoopFile("hdfs://localhost:9000/user/zzg/Info-00-sortByTime",
      classOf[LongWritable],
      classOf[Text],
      classOf[SequenceFileOutputFormat[LongWritable,Text]])
  /*   .saveAsHadoopFile("hdfs://localhost:9000/user/zzg/Info-00-sortByTime",classOf[LongWritable],classOf[Text],
     classOf[TextOutputFormat[LongWritable,Text]])*/
  }

  def myCompare( x :String){

  }

  def mapCars(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val carIDs = sc.textFile("/home/zzg/IdeaProjects/second/taxis").collect();
    util.Sorting.quickSort(carIDs)
    for(i<-0 to carIDs.length-1){
      println(carIDs.apply(i)+"\t"+i)
    }
    /* val maps:Map[String,Int] = Map()
     for(i<-0 to carIDs.length-1){
       maps += (carIDs.apply(i)->i)
     }
     val writer = new FileWriter((new File("~/result"))
     */
  }

  def getAllCars(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val set = sc.textFile("hdfs://localhost:9000/user/zzg/beijing_taxi/2013-10-*/Info-*",13).map(x=>{
      val seq = x.split(",").toSeq
      seq.apply(1)
    }).distinct(13).collect()
    for(i<-0 to set.length-1){
      println(set.apply(i))
    }

    print("total length: "+set.length)
  }

  //1->0,1,2,3
  //2->1
  def staticAttrs(): Unit ={
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[3]")
    val sc = new SparkContext(conf)
   // val set = sc.textFile("/home/zzg/t").flatMap(x=>{
/*  /*  val set = sc.textFile("hdfs://localhost:9000/user/zzg/beijing_taxi/2013-10-*/Info-*",13).flatMap(x=>{
      val tmp = x.split(",")
      val a1 = tmp(6).toByte
      val a2 = tmp(7).toByte
      val a3= tmp(8).toByte
      Seq((1,a1),(2,a2),(3,a3))
    }).distinct().collect().foreach(println)
*/
     sc.textFile("hdfs://localhost:9000/user/zzg/beijing_taxi/2013-10-*/Info-*",13).map(x=>{
      val tmp = x.split(",")
       val a3 = tmp(8).toByte
      (3,a3)
    }).distinct().collect().foreach(println)
  }
}
