package edu.ecnu.idse.TrajStore.Taxi

import java.io._
import java.net.URI
import java.text.SimpleDateFormat

import edu.ecnu.idse.TrajStore.core.{CellInfo, MoPoint, Rectangle, SpatialTemporalSite}
import edu.ecnu.idse.TrajStore.util.SpatialUtilFuncs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.io.{NullWritable, Text, WritableComparable}
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by zzg on 16-1-28.
  */
class Block(var blockRegion:CellInfo,var idHash:Int,
            var beginTime:Long )
            extends WritableComparable[Block]{

//  var blockRegion = new CellInfo()
  // cell info= id:4 location: 16
  // id hash:4
  // begintime:8
  // block size: 8
  //end time :8
  // trajectory num:4
  var BlockSize=52l
  var endTime = beginTime
  var trajSet = new ArrayBuffer[Trajectory]()

  // need to be tested

  def this()=this(new CellInfo(),-1,0)

  def getEndTime: Long ={
    beginTime
  }

  def getBeginTime():Long={
    endTime
  }

  def addTaxiMS(taxiMs: TaxiMs): Unit ={
    if(endTime < taxiMs.getTimeStamp()){
      endTime = taxiMs.getTimeStamp()
    }
    val index2Insert = BinarySearchIndexToInsert(taxiMs.getID(),trajSet.toArray)
    if(index2Insert._1==false){
// this mo have no records stored
      val trajNew = new Trajectory(taxiMs)
      trajSet.insert(index2Insert._2,trajNew)
      BlockSize = BlockSize+ trajNew.getTotalBytes()
    }else{
        // exists
      val preSize = trajSet(index2Insert._2).totalBytes
      trajSet(index2Insert._2).addRecord(taxiMs)
      val afterSize = trajSet(index2Insert._2).totalBytes
      BlockSize = BlockSize -preSize+afterSize
    }
  }

  def getToTalBytes(): Long ={
      BlockSize
  }
//  def addTrajectory(trajectory: Trajectory): Unit ={
//     val index2Insert = BinarySearchIndexToInsert(trajectory.getTrajectoryID,trajSet.toArray)
//    if(index2Insert.isDefined){
//      trajSet.insert(index2Insert.get,trajectory)
//    }
//
//  }

  def print: Unit ={
    println(blockRegion.toString)
    println(idHash)
    println(BlockSize)
    println("Block begin time:"+beginTime)
    println("Block end time:"+endTime)
    for(traj <- trajSet.toArray){
      traj.printTrajectory()
    }
  }

  def getTrajectory(trajectoryID:Int):Option[Trajectory]={
    BinarySearch(trajectoryID,trajSet.toArray)
  }

  // need to be tested
  def BinarySearchIndexToInsert(id:Int,arrays: Array[Trajectory]):Pair[Boolean,Int]={
    var start = 0
    var end = arrays.length -1
    var mid = 0
    var midValue = 0l
    var tuple : Pair[Boolean,Int] = new Pair[Boolean,Int](false,-1)
    var result:Option[Int] = None
    if(arrays.length==0){
      tuple=(false,0)
    }else{
      breakable {
        while (start <= end) {
          mid = (start + end) >> 1
          midValue = arrays(mid).getTrajectoryID
          if (midValue == id) {
            break
          } else if (id < midValue) {
            end = mid - 1
          } else {
            start = mid + 1
          }
        }
      }
      // if the trajectory is existed, return the id
      if(arrays(start).getTrajectoryID==id)
        tuple=(true,mid)
      else
        tuple=(false,start)
    }

    tuple
  }


  // need to be test
  def BinarySearch(id:Int,arrays: Array[Trajectory]):Option[Trajectory]={
    var tra:Option[Trajectory] = None
    var start = 0
    var end = arrays.length -1
    var mid = (start+end)>>1
    var midValue = arrays(mid).getTrajectoryID

    breakable {
      while (start <= end) {
        mid = (start + end) >> 1
        midValue = arrays(mid).getTrajectoryID
        if (midValue == id) {
          break
        } else if (id < midValue) {
          end = mid - 1
        } else {
          start = mid + 1
        }
      }
    }
    if(midValue == id){
      tra =Some(arrays(mid))
    }else{
      tra = None
    }
    tra
  }

  /*
  To find cars in the given spatial and temporal region
   */
  def SpatialTemporalQuery(MinX: Double,MaxX: Double,MinY: Double,MaxY: Double,BeginTime:Long, EndTime:Long): Option[Array[Int]] ={
    SpatialTemporalQuery(new Rectangle(MinX,MaxX,MinY,MaxY),BeginTime,EndTime)
  }

  def SpatialTemporalQuery(rect:Rectangle,BeginTime:Long, EndTime:Long): Option[Array[Int]] ={
    var resultBuffer: Option[Array[Int]] = None
    if(!blockRegion.getMBR.isIntersected(rect)|| BeginTime>endTime || EndTime<BeginTime )
      resultBuffer = None
//    val resultBuffer = new ArrayBuffer[Int]()
    else{
      val trajIDResults = new ArrayBuffer[Int]()
      for(traj <- trajSet){
        if(traj.getSpatialTemporal(rect,BeginTime,EndTime).isDefined){
          trajIDResults += traj.getTrajectoryID
        }
      }
      resultBuffer = Some(trajIDResults.toArray)
    }

    resultBuffer
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput): Unit ={
    blockRegion.readFields(in)
    idHash= in.readInt()
    beginTime = in.readLong()
    endTime = in.readLong()
    BlockSize = in.readLong()
    val num = in.readInt()
    trajSet.clear()
    trajSet.sizeHint(num)
    for(i<-0 until num){
      val FMS = new TaxiMs()
      val tmpTrajectory = new Trajectory(FMS)
      tmpTrajectory.readFields(in)
      trajSet += tmpTrajectory
    }

  }

  override def write(out: DataOutput): Unit = {
    blockRegion.write(out)
    out.writeInt(idHash)
    out.writeLong(beginTime)
    out.writeLong(endTime)
    out.writeLong(BlockSize)
    val length = trajSet.toArray.length
    out.writeInt(length)
    for(i<-0 until length){
      trajSet(i).write(out)
    }
  }

  // need to be test
  override def compareTo(bc: Block):Int={
    var value = 0
      if(blockRegion.cellId < bc.blockRegion.cellId)
        value= -1
    else if(blockRegion.cellId> bc.blockRegion.cellId)
        value =1
    else{
        if(beginTime< bc.beginTime)
          value = -1
        else if(beginTime>bc.beginTime)
          value=1
        else
          value =0
      }
    value
  }
}

object Block{
  def main(args: Array[String]) {
   test() //sequeceoutputformat
  //  test2()
   //testArrrayBuffer
    getPaths
  }

  def testArrrayBuffer(): Unit ={
    val arrayBuffer = new ArrayBuffer[Int]()
    arrayBuffer+=1
    arrayBuffer+=2
    println(arrayBuffer.length)

    arrayBuffer.clear()
    println(arrayBuffer.length)
    arrayBuffer.sizeHint(10)
    arrayBuffer+=11
    println(arrayBuffer.length)
  }
  def test2(): Unit ={
    val hBaseConf = HBaseConfiguration.create()
    val indexes = SpatialTemporalSite.ReadSpatialIndex(hBaseConf)
    val sparkConf = new SparkConf().setAppName("BlockTest").setMaster("local[4]")
    sparkConf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[TaxiMs],classOf[Text],classOf[Trajectory],classOf[Block]))

    val sc  = new SparkContext(sparkConf)

    val carMap = sc.textFile("hdfs://localhost:9000/user/zzg/id-map").map(x=>{
      val tmp =x.split("\t")
      (tmp.apply(0),tmp.apply(1).toInt)
    }).collectAsMap()

    val outPath="hdfs://localhost:9000/user/zzg/res"
    val fileSystem = FileSystem.get(URI.create(outPath),hBaseConf)
    //   val out = fileSystem.create(new Path(outPath),true)-
    val out = new Path(outPath)
    if(fileSystem.exists(out)){
      fileSystem.delete(out,true)
    }
    val sd = new SimpleDateFormat("yyyyMMddHHmmss")
    val beginTime = sd.parse("20131001000000").getTime

    val broadcastVal = sc.broadcast(beginTime)
    val broadCars = sc.broadcast(carMap)
    val bcIndexes = sc.broadcast(indexes)

    val records = sc.textFile("hdfs://localhost:9000/user/zzg/car1")

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
    }).groupByKey(1)
      .map(x=>{

        val carRange = x._1.toInt & 0xffffffff
        val region = x._1 >> 32
        println("region: "+region)
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
        val cellInfo =   SpatialUtilFuncs.getCellInfo(region.toInt,bcIndexes.value)
        val block= new Block(cellInfo,carRange,recordSortedByTime(0).getTimeStamp())
        for(taxiMS <- recordSortedByTime){
          block.addTaxiMS(taxiMS)
          println( block.getToTalBytes())
        }

        block.print

        var endTime = recordSortedByTime(0).getTimeStamp()
        if(recordSortedByTime.length>1)
          endTime = recordSortedByTime(recordSortedByTime.length -1).getTimeStamp()

        val namePath = "hdfs://localhost:9000/user/zzg/Blocks/" + region + "-" + carRange + "-" +
          recordSortedByTime(0).getTimeStamp() + "-" + endTime

        val conf =  new Configuration()
        val fileSystem = FileSystem.get(URI.create(namePath),conf)
        val out = fileSystem.create(new Path(namePath),true)
        //  val bw = new BufferedWriter(new OutputStreamWriter(out))
        // total size 1403
        block.write(out)
        out.close()
        block
      })

 //   getPaths

    sc.stop()
  }

  def getPaths(): Unit ={
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)
//    val path = new Path("hdfs://localhost:9000/user/zzg/Blocks/")
    val path = new Path("file:///home/zzg/Blocks/")
    val fStatus = hdfs.listStatus(path)
    for(i<-0 until fStatus.length){
      val tmpName = fStatus(i).getPath.toString
      println(tmpName)
      val in = hdfs.open(new Path(tmpName))
      val bloc = new Block()
      bloc.readFields(in)
      bloc.print
    }
  }

  def test(): Unit ={
    val hBaseConf = HBaseConfiguration.create()
    val indexes = SpatialTemporalSite.ReadSpatialIndex(hBaseConf)
    val sparkConf = new SparkConf().setAppName("BlockTest").setMaster("local[4]")
    sparkConf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[TaxiMs],classOf[Text],classOf[Trajectory],classOf[Block]))

    val sc  = new SparkContext(sparkConf)

    val carMap = sc.textFile("hdfs://localhost:9000/user/zzg/id-map").map(x=>{
      val tmp =x.split("\t")
      (tmp.apply(0),tmp.apply(1).toInt)
    }).collectAsMap()

    val outPath="hdfs://localhost:9000/user/zzg/res"
    val fileSystem = FileSystem.get(URI.create(outPath),hBaseConf)
    //   val out = fileSystem.create(new Path(outPath),true)-
    val out = new Path(outPath)
    if(fileSystem.exists(out)){
      fileSystem.delete(out,true)
    }
    val sd = new SimpleDateFormat("yyyyMMddHHmmss")
    val beginTime = sd.parse("20131001000000").getTime

    val broadcastVal = sc.broadcast(beginTime)
    val broadCars = sc.broadcast(carMap)
    val bcIndexes = sc.broadcast(indexes)

    val records = sc.textFile("hdfs://localhost:9000/user/zzg/car2")

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
        val cellInfo =   SpatialUtilFuncs.getCellInfo(region.toInt,bcIndexes.value)
        val block= new Block(cellInfo,carRange,recordSortedByTime(0).getTimeStamp())
        for(taxiMS <- recordSortedByTime){
          block.addTaxiMS(taxiMS)
        }

        println("print block")
        block.print

        var endTime = recordSortedByTime(0).getTimeStamp()
        if(recordSortedByTime.length>1)
          endTime = recordSortedByTime(recordSortedByTime.length -1).getTimeStamp()

        val namePath = "hdfs://localhost:9000/user/zzg/Blocks/" + region + "-" + carRange + "-" +
          recordSortedByTime(0).getTimeStamp() + "-" + endTime

        val conf =  new Configuration()
        val fileSystem = FileSystem.get(URI.create(namePath),conf)
        val out = fileSystem.create(new Path(namePath),true)
      //  val bw = new BufferedWriter(new OutputStreamWriter(out))
        // total size 1403
        block.write(out)
        out.close()
        /*
          write with sequence file


         */
        (NullWritable.get(), block)
      }).saveAsHadoopFile("hdfs://localhost:9000/user/zzg/res",classOf[NullWritable],classOf[Block],classOf[SequenceFileOutputFormat[NullWritable,Block]])
   // two read methods 1: save all blocks in a file
    //                 2: save each block as a single block
    println("read files")

    // method 1
/*    val blocks = sc.hadoopFile("hdfs://localhost:9000/user/zzg/res",
      classOf[SequenceFileInputFormat[NullWritable,Block]],
      classOf[NullWritable],
      classOf[Block])

    val values = blocks.values
    values.foreach(x=>x.print)*/
    // method 2
    getPaths
    sc.stop()
  }

}
