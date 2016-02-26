package edu.ecnu.idse.TrajStore.Taxi

import java.io.{DataInput, DataOutput, IOException}
import java.text.SimpleDateFormat

import edu.ecnu.idse.TrajStore.core._
import edu.ecnu.idse.TrajStore.util.SpatialUtilFuncs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{Text, WritableComparable}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by zzg on 15-12-29.
  */

// class TaxiMessage(region:Long,time:Long,id:Int,longitude:Float,latitude:Float,speed:Int)extends Serializable
//@deprecated
case class TaxiMessage(var region:Long,var time:Long,var id:Int,var longitude:Float,var latitude:Float,var speed:Int)


object TaxiMessage extends Serializable{

  final val family = Bytes.toBytes("as")
  final  val c_speed = Bytes.toBytes("s")
  final  val c_log = Bytes.toBytes("lo")
  final  val c_lat = Bytes.toBytes("la")

  def testArrayBufferr(): Unit ={
    var a = Array(0 until(3))
    a.foreach(println)
    var arrayBuffer :Option[ArrayBuffer[Int]] = None
    val arrs = new ArrayBuffer[Int]
    arrs +=10
    arrs ++= (0 to 3)
    arrayBuffer = Some(arrs)
    println( arrayBuffer.get.mkString(","))

  }

  def testSearch(): Unit ={
    /*    val as = Array(1l,3l,5l,7l,9l)
     val index =  binarySearch(4,as)
      println(as(index))*/
    /*
        val index =  binarySearch(4,as)
        println(as(index))

        val index1 =  binarySearch(5,as)
        println(as(index1))
        val index2 =  binarySearch(3,as)
        println(as(index2))

        val index3 =  binarySearch(1,as)
        println(as(index3))

        val index4 =  binarySearch(9,as)
        println(as(index4))

        val index5 =  binarySearch(2,as)
        println(as(index5))

        val index6=  binarySearch(6,as)
        println(as(index6))*/
    /*val as = Array(1,3,5,7,9)
        var range = getTimeRange(2,6,as)
        println(range)
        var range1 = getTimeRange(-1,6,as)
        println(range1)
        var range2 = getTimeRange(-1,11,as)
        println(range2)
        var range3 = getTimeRange(5,11,as)
        println(range3)
        var range4 = getTimeRange(11,21,as)
        println(range4)
        var range5 = getTimeRange(-1,0,as)
        println(range5)*/
  }

  def TrajecWrite: Unit ={
    var hadoopConf = new Configuration()
    var sparkConf = new SparkConf().setMaster("local[4]").setAppName("DataReceiver")

    sparkConf.set("spark.serializer", "org,apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[CellInfo], classOf[Text]))

    val sc = new SparkContext(sparkConf)
    val indexs = SpatialTemporalSite.ReadSpatialIndex(hadoopConf)
    val carMap = sc.textFile("hdfs://localhost:9000/user/zzg/id-map").map(x=>{
      val tmp =x.split("\t")
      (tmp.apply(0),tmp.apply(1).toInt)
    }).collectAsMap()
    val brIndex = sc.broadcast(indexs)
    val brCar = sc.broadcast(carMap)
    val sd = new SimpleDateFormat("yyyyMMddHHmmss")
    val beginTime = sd.parse("20131001000000").getTime
    val brBegin = sc.broadcast(beginTime)
    val records = sc.textFile("/home/zzg/car1").map(x=>TaxiMs.parseMessage(x,brIndex.value,brBegin.value,brCar.value))
    records.foreach(println)
    val tras =  records.groupByKey(2).map(x=>{

      def sortWithTime(taxiMs1: TaxiMs,taxiMs2: TaxiMs):Boolean = {
        val t1 = taxiMs1.getTimeStamp()
        val t2 = taxiMs2.getTimeStamp()
        if(t1<t2){
          true
        }else{
          false
        }
      }
      //将同一个id的数据划分到一起
      val recordSortedByTime  = x._2.toList.sortWith(sortWithTime)
      val mapTraj = new mutable.HashMap[Int,Trajectory]()
      for(taxiMS<-recordSortedByTime){
        if(mapTraj.contains(taxiMS.getID())){
          val tmpTraj =  mapTraj.get(taxiMS.getID()).get
          //添加
          tmpTraj.addRecord(taxiMS)
        }else{//不存在
        val  newTraj = new Trajectory(taxiMS)
          mapTraj.put(taxiMS.getID(),newTraj)
        }
      }
      mapTraj
    })
    tras.foreach(x=>{
      x.foreach(traMap=>{
        traMap._2.printTrajectory()
      //  println(traMap._1,traMap._2)
      })
    })
  }



  def testAttributeValue: Unit ={
    val a1s = new ArrayBuffer[Byte]()
    a1s+=0
    val nums = Array[Byte](1,2,3,0,0,2,1)
    for(number<-0 until(nums.length)){
      if( number*2/8 >= a1s.length)
        a1s+=0
      a1s(number*2/8)= (a1s(number*2/8) | nums(number)<<(number*2)%8).toByte
      println(nums(number)+"\t"+a1s(number*2/8))
    }

    for(index<-0 until nums.length){
      val vindex = a1s(index*2/8)
      val loc = index*2%8
      val b = 3.toByte
      val result = ((vindex>>(loc)) & b).toByte
      println(result)
    }

  }

  def testAttribute2: Unit ={
    val a1s = new ArrayBuffer[Byte]()
    a1s+=0
    val nums = Array[Byte](1,0,1,1,0,1,0,1,0,0,1)
    for(number<-0 until(nums.length)){
      if( number/8 >= a1s.length)
        a1s+=0
      a1s(number/8)= (a1s(number/8) | nums(number)<<(number)%8).toByte
      println(nums(number)+"\t"+a1s(number/8))
    }
    for(index<-0 until nums.length){
      val vindex = a1s(index/8)
      val loc = index%8
      val b = 1.toByte
      val result = ((vindex>>(loc)) & b).toByte
      println(result)
    }

  }

  def main(args: Array[String]) {

    // testAttribute2
    //TrajecWriite
 //   testAttributeValue

  }

  def binarySearch(value: Long,arrays:Array[Long]): Int ={
    var start = 0
    var end = arrays.length -1
    var middle = 0
    var midvalue = 0l
    while(start<=end){
      middle = (start+end)>>1
      midvalue = arrays(middle)
      if(midvalue==value){
        return middle
        break
      } else if(value < midvalue){
        end = middle -1
      }else{
        start = middle+1
      }
    }
   //  start
    end
  }

  def binarySearchUp(value: Int,arrays:Array[Int]): Int ={
    var start = 0
    var end = arrays.length -1
    var middle = 0
    var midvalue = 0l
    while(start<=end){
      middle = (start+end)>>1
      midvalue = arrays(middle)
      if(midvalue==value){
        return middle
      } else if(value < midvalue){
        end = middle -1
      }else{
        start = middle+1
      }
    }
    start
  }

  def binarySearchDown(value: Int,arrays:Array[Int]): Int ={
    var start = 0
    var end = arrays.length -1
    var middle = 0
    var midvalue = 0l
    while(start<=end){
      middle = (start+end)>>1
      midvalue = arrays(middle)
      if(midvalue==value){
        return middle
      } else if(value < midvalue){
        end = middle -1
      }else{
        start = middle+1
      }
    }
    end
  }

  def getTimeRange(minTime :Int,maxTime:Int,zDiffs:Array[Int]): Option[(Int,Int)] ={
    var result:Option[(Int,Int)] = None
    val minz = zDiffs(0)
    val maxz = zDiffs(zDiffs.length -1)
    if(minTime> maxz || maxTime< minz)
      result = None
    else if(minz<minTime && maxz>minTime && maxz<maxTime){
      val bt = binarySearchUp(minTime,zDiffs)
      result = Some((bt,zDiffs.length))
      //find the first record which has a time larger than minTime
    }else if(minz> minTime && minz<maxTime && maxz > maxTime ){
      val et = binarySearchDown(maxTime,zDiffs)
      result = Some((0,et))
    } else if(minz>minTime && maxz<maxTime){
      result = Some((0,zDiffs.length))
    }else {
      val bt = binarySearchUp(minTime,zDiffs)
      val et = binarySearchDown(maxTime,zDiffs)
      result = Some((bt,et))
    }
    result
  }

  def parseMessage(str: String,index :Array[CellInfo]):TaxiMessage={
    val temp= str.split("\t")
    if(temp.length!=2)
      throw new IllegalArgumentException("read data:"+temp+" error, because its elements is not two")
    val attrs = temp(1).split(",")
    val carID = attrs(0).toShort
    val longitude = attrs(1).toFloat
    val latitude = attrs(2).toFloat
    val region = SpatialUtilFuncs.getLocatedRegion(longitude,latitude,index)
    val speed = attrs(3).toFloat.toInt

    if(region.equals(-1)){
     new TaxiMessage(Long.MaxValue, temp(0).toLong,carID, longitude, latitude, speed)
    }else{
      val hashCarID = carID%7

      val split = (region.toLong <<32) + hashCarID          //region:hashCarID

     new TaxiMessage(split, temp(0).toLong,carID, longitude, latitude, speed)
      //split time           id      logi      lat      spped
    }

  }

  def convertToPut(ms: TaxiMessage):(ImmutableBytesWritable,Put)={
    val key = Bytes.toBytes(ms.region) ++ Bytes.toBytes(ms.id) ++ Bytes.toBytes(ms.time)
    val put = new Put(key)
    /*    final val family = Bytes.toBytes("as")
        final  val c_speed = Bytes.toBytes("s")
        final  val c_log = Bytes.toBytes("lo")
        final  val c_lat = Bytes.toBytes("la")*/
    put.add(family,c_log,Bytes.toBytes(ms.longitude))
    put.add(family,c_lat,Bytes.toBytes(ms.latitude))
    put.add(family,c_speed,Bytes.toBytes(ms.speed))
    return (new ImmutableBytesWritable(key),put)
  }

}