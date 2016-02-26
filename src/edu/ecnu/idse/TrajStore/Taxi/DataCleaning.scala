package edu.ecnu.idse.TrajStore.Taxi

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Random

import edu.ecnu.idse.TrajStore.core.{MoPoint, SpatialTemporalSite}
import edu.ecnu.idse.TrajStore.util.SpatialUtilFuncs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zzg on 15-12-29.
  */
object DataCleaning {
//  val numPerSplit = 2*1024*1024
val numPerSplit = 20000
  val sizeThreshold = 32*1024
  def main(args: Array[String]): Unit = {
  //  var res =   3895535337472l
  /*  val region = 1l
    val hash  = 7
    val tmp = region<<2
    println(tmp)
    val split =( region<<32) + hash
    println(split)


   println(split.toHexString)
    println(split>>32)
    println(split.toInt & 0xffffffff)
    println(split & 0xffff)*/

  //test
 //  filePathBuild
  //    filePathWithObj
      filePathWithObjTest
/*    val hBaseConf  = HBaseConfiguration.create()
    val indexs = SpatialTemporalSite.ReadSpatialIndex(hBaseConf)
    for( c <- indexs){
      println(c)
    }*/
  }

  def test(): Unit ={
    val path ="hdfs://localhost:9000/user/zzg/scalaHDFS"
    val conf =  new Configuration()
    val fileSystem = FileSystem.get(URI.create(path),conf)
    val out = fileSystem.create(new Path(path),true)
    val br = new BufferedWriter(new OutputStreamWriter(out))
    val random = new Random()
    for(i<-0 until 10){
      br.write(random.nextInt()+"\n")
    }
    br.close()
  }

  def filePathWithObjTest: Unit ={
    val hBaseConf  = HBaseConfiguration.create()
    val indexes = SpatialTemporalSite.ReadSpatialIndex(hBaseConf)

    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[4]")
    conf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[TaxiMs],classOf[Text]))
    val sc = new SparkContext(conf)
    val record = sc.sequenceFile("hdfs://localhost:9000/user/zzg/TransRec/984-984-2-35555",classOf[TaxiMs],classOf[NullWritable]).keys
    record.foreach(x=>(println(x)))
    record.filter(x=>x.getTimeStamp()<100).foreach(x=>println(x))
  }

  def filePathWithObj: Unit ={
    val hBaseConf  = HBaseConfiguration.create()
    val indexes = SpatialTemporalSite.ReadSpatialIndex(hBaseConf)

    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[4]")
    conf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LongWritable],classOf[Text]))
    val sc = new SparkContext(conf)

    val carMap = sc.textFile("hdfs://localhost:9000/user/zzg/id-map").map(x=>{
      val tmp =x.split("\t")
      (tmp.apply(0),tmp.apply(1).toInt)
    }).collectAsMap()

    val outPath="hdfs://localhost:9000/user/zzg/res"
    val fileSystem = FileSystem.get(URI.create(outPath),hBaseConf)
 //   val out = fileSystem.create(new Path(outPath),true)
    val out = new Path(outPath)
    if(fileSystem.exists(out)){
        fileSystem.delete(out,true)
    }
    val sd = new SimpleDateFormat("yyyyMMddHHmmss")
    val beginTime = sd.parse("20131001000000").getTime

    val broadcastVal = sc.broadcast(beginTime)
    val broadCars = sc.broadcast(carMap)
    val bcIndexes = sc.broadcast(indexes)

    val records = sc.textFile("hdfs://localhost:9000/user/zzg/Info-00")
    val bcNumPerSplit = sc.broadcast(numPerSplit)
    records.map(x=> {
      val tmp = x.split(",")
      // println(tmp.apply(0))
      val localSD = new SimpleDateFormat("yyyyMMddHHmmss")
      //20131001000159,001140,116.406975,39.988724,45.000000,264.000000,1,1,0,20131001000159
      val tNow = localSD.parse(tmp.apply(0)).getTime
      val time1 = (tNow - broadcastVal.value) / 1000
      val tEnd = localSD.parse(tmp.apply(9)).getTime
      val time2 = ((tEnd - broadcastVal.value) / 1000)

      val cid = broadCars.value.get(tmp.apply(1)).get
      val log = tmp(2).toFloat
      val lat = tmp(3).toFloat
      val region = SpatialUtilFuncs.getLocatedRegion(log,lat,bcIndexes.value)
      val speed = tmp(4).toFloat.toShort
      val angle = tmp(5).toFloat.toShort
      val a1 = tmp(6).toByte
      val a2 = tmp(7).toByte
      val a3= tmp(8).toByte
      val hashCarID = cid%7
      val split = (region.toLong <<32) + hashCarID
      (split,new TaxiMs(new MoPoint(cid,log,lat,time1),speed,angle,a1,a2,a3))
    //  (split,(time1,cid,log,lat,speed,angle,a1,a2,a3,time2))
    }).groupByKey(13)
    .map(x=>{

      val carRange = x._1.toInt & 0xffffffff
      val region = x._1 >> 32

      def sortWithTime(taxiMs1: TaxiMs,taxiMs2: TaxiMs):Boolean = {
        val t1 = taxiMs1.getTimeStamp()
        val t2 = taxiMs2.getTimeStamp()
        if(t1<t2){
          true
        }else{
          false
        }
      }

      val recordSortedByTime  = x._2.toList.sortWith(sortWithTime)
      val recordNum = recordSortedByTime.length
      val splits = recordNum / bcNumPerSplit.value


      for (i <- 0 until splits) {

        val sliceRecords = recordSortedByTime.slice(i * bcNumPerSplit.value, (i + 1) * bcNumPerSplit.value)
        val namePath = "hdfs://localhost:9000/user/zzg/TransRec/" + region + "-" + carRange + "-" + sliceRecords(i * bcNumPerSplit.value).getTimeStamp() + "-" + sliceRecords((i + 1) * bcNumPerSplit.value - 1).getTimeStamp()
        writeMSToHdfs(sliceRecords, namePath)
      }
      if(splits * bcNumPerSplit.value < recordNum){
        val sliceRecords = recordSortedByTime.slice(splits * bcNumPerSplit.value, recordNum)
        val namePath = "hdfs://localhost:9000/user/zzg/TransRec/" + region + "-" + carRange + "-" + recordSortedByTime(splits * bcNumPerSplit.value).getTimeStamp() + "-" + recordSortedByTime(recordNum - 1).getTimeStamp()
        writeMSToHdfs(sliceRecords, namePath)
      }
 /*val text = new Text("")
   var preBegin= recordSortedByTime(i).moPoint.getTimeStamp
    //  recordSortedByTime(i).toText(text)
      while(i<recordSortedByTime.length ){
        if(text.getLength<sizeThreshold ){
          //     recordSortedByTime(i).toText(text)
          i= i+1
        }else{

          val path = "hdfs://localhost:9000/user/zzg/TransRec/" + region + "-" + carRange + "-"+preBegin+"-" +recordSortedByTime(i).moPoint.getTimeStamp
          val conf =  new Configuration()
          val fileSystem = FileSystem.get(URI.create(path),conf)
          val out = fileSystem.create(new Path(path),true)
          text.write(out)

          text.clear()
          i= i+1
          if(i<recordSortedByTime.length){
            preBegin = recordSortedByTime(i).moPoint.getTimeStamp
          }
        }
      }*/
    }).saveAsTextFile("hdfs://localhost:9000/user/zzg/res")
    sc.stop()
  }



  // eacg records as a line
  def filePathBuild(): Unit ={

    val hBaseConf  = HBaseConfiguration.create()
    val indexs = SpatialTemporalSite.ReadSpatialIndex(hBaseConf)

    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[4]")
    conf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LongWritable],classOf[Text]))
    val sc = new SparkContext(conf)

    val carMap = sc.textFile("hdfs://localhost:9000/user/zzg/id-map").map(x=>{
      val tmp =x.split("\t")
      (tmp.apply(0),tmp.apply(1).toInt)
    }).collectAsMap()

    val sd = new SimpleDateFormat("yyyyMMddHHmmss")
    val beginTime = sd.parse("20131001000000").getTime

    val broadcastVal = sc.broadcast(beginTime)
    val broadCars = sc.broadcast(carMap)
    val bcIndexes = sc.broadcast(indexs)
    val bcNumPerSplit = sc.broadcast(numPerSplit)

    val records = sc.textFile("hdfs://localhost:9000/user/zzg/Info-00")
  /* records.map(x=>x.length).saveAsTextFile("hdfs://localhost:9000/user/zzg/res")
    println("finsihe")*/
    records.map(x=> {
      val tmp = x.split(",")
      // println(tmp.apply(0))
      val localSD = new SimpleDateFormat("yyyyMMddHHmmss")
//20131001000159,001140,116.406975,39.988724,45.000000,264.000000,1,1,0,20131001000159
        val tNow = localSD.parse(tmp.apply(0)).getTime
        val time1 = (tNow - broadcastVal.value) / 1000
        val tEnd = localSD.parse(tmp.apply(9)).getTime
        val time2 = ((tEnd - broadcastVal.value) / 1000)

        val cid = broadCars.value.get(tmp.apply(1)).get
        val log = tmp(2).toFloat
        val lat = tmp(3).toFloat
        val region = SpatialUtilFuncs.getLocatedRegion(log,lat,bcIndexes.value)
        val speed = tmp(4).toFloat.toShort
        val angle = tmp(5).toFloat.toShort
        val a1 = tmp(6).toByte
        val a2 = tmp(7).toByte
        val a3= tmp(8).toByte
        val hashCarID = cid%7
        val split = (region.toLong <<32) + hashCarID          //region:hashCarID
        (split,(time1,cid,log,lat,speed,angle,a1,a2,a3,time2))
      })/*.take(10).foreach(println)*/
     .groupByKey(2)
    .map(x=> {
      val sortedByTimeRecords = x._2.toList.sortWith(_._1 < _._1)
      val recordNum = sortedByTimeRecords.length
      val splits = recordNum / bcNumPerSplit.value
      val carRange = x._1.toInt & 0xffffffff
      val region = x._1 >> 32

      for (i <- 0 until splits) {

        val sliceRecords = sortedByTimeRecords.slice(i * bcNumPerSplit.value, (i + 1) * bcNumPerSplit.value)
        val namePath = "hdfs://localhost:9000/user/zzg/TransRec/" + region + "-" + carRange + "-" + sortedByTimeRecords(i * bcNumPerSplit.value)._1 + "-" + sortedByTimeRecords((i + 1) * bcNumPerSplit.value - 1)._1
        writeToHdfs(sliceRecords, namePath)
      }
      if(splits * bcNumPerSplit.value < recordNum){
        val sliceRecords = sortedByTimeRecords.slice(splits * bcNumPerSplit.value, recordNum)
        val namePath = "hdfs://localhost:9000/user/zzg/TransRec/" + region + "-" + carRange + "-" + sortedByTimeRecords(splits * bcNumPerSplit.value)._1 + "-" + sortedByTimeRecords(recordNum - 1)._1
        writeToHdfs(sliceRecords, namePath)
      }
     (x._1,recordNum)
//    (x._1,x._2.toList.sortWith(_._1<_._1))  //all record in a split sorted with time first
  }).saveAsTextFile("hdfs://localhost:9000/user/zzg/res")
    println("finished")
  }

  def writeToHdfs(recList:List[(Long,Int,Float,Float,Short,Short,Byte,Byte,Byte,Long)],path:String): Unit ={
    val conf =  new Configuration()
    val fileSystem = FileSystem.get(URI.create(path),conf)
    val out = fileSystem.create(new Path(path),true)
    val bw = new BufferedWriter(new OutputStreamWriter(out))
    for(i<-0 until  recList.length){
      bw.write(formateRecord(recList(i)))
    }
    bw.close()

  }

  def writeMSToHdfs(recList:List[TaxiMs],pa:String): Unit ={
    val conf = new Configuration()
    val fs =  FileSystem.get(conf)
    val path = new Path(pa)
    val writer = SequenceFile.createWriter(conf,Writer.file(path),Writer.keyClass(classOf[TaxiMs]),Writer.valueClass(classOf[NullWritable]))
    recList.foreach(x=>writer.append(x,NullWritable.get()))
    IOUtils.closeStream(writer)
  }

  def formateRecord(rec :(Long,Int,Float,Float,Short,Short,Byte,Byte,Byte,Long)):String = {

    rec._1+","+rec._2+","+rec._3+","+rec._4+","+ rec._5+","+rec._6+","+ rec._7+","+rec._8+","+rec._9+","+rec._10+"\n"
  }



  def wholeDataProcessing(): Unit ={
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[4]")
    conf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LongWritable],classOf[Text]))
    val sc = new SparkContext(conf)

    val carMap = sc.textFile("hdfs://localhost:9000/user/zzg/id-map").map(x=>{
      val tmp =x.split("\t")
      (tmp.apply(0),tmp.apply(1).toInt)
    }).collectAsMap()

    val sd = new SimpleDateFormat("yyyyMMddHHmmss")
    val beginTime = sd.parse("20131001000000").getTime
    val records = sc.textFile("hdfs://localhost:9000/user/zzg/begiing_taxi/20*/Info*")
  }


}
