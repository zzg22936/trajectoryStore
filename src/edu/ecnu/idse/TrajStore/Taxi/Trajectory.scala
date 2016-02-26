package edu.ecnu.idse.TrajStore.Taxi

import java.io.{DataOutput, DataInput, IOException}

import edu.ecnu.idse.TrajStore.core.{Point, Rectangle, MoPoint}
import org.apache.hadoop.io.WritableComparable

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zzg on 16-1-29.
  */
class Trajectory(var FMS:TaxiMs) extends WritableComparable[Trajectory]{

  var number:Int=1
  val xDiffs = new ArrayBuffer[Int](10)
  xDiffs+=0
  val yDiffs = new ArrayBuffer[Int](10)
  yDiffs+=0
  val zDiffs = new ArrayBuffer[Int](10)
  zDiffs+=0
// range+ size of FMS + diffs (initial)
  var totalBytes= 48+35+3*4+2*2+3l

  var minx:Double= FMS.getLongitude()
  var maxx:Double= FMS.getLongitude()
  var miny:Double= FMS.getLatitude()
  var maxy:Double= FMS.getLatitude()
  var minz:Long= FMS.getTimeStamp()
  var maxz:Long= FMS.getTimeStamp()

  var speeds= new ArrayBuffer[Short](1)
  speeds+=0
  var angles= new ArrayBuffer[Short](1)
  angles+=0
  val a1s = new ArrayBuffer[Byte]((number - 1)*2/8 +1)
  a1s+= FMS.a1
  val a2s= new ArrayBuffer[Byte]((number-1)/8+1)
  a2s+=FMS.a2
  val a3s= new ArrayBuffer[Byte]((number-1)/8+1)
  a3s+= FMS.a3

  def getTotalBytes(): Long ={
      totalBytes
  }

  def printTrajectory(): Unit ={
    println("record num:" +number)
    println("xdiffs length" + xDiffs.length)
    println("trajectory total bytes "+totalBytes)
    println(FMS.toString())

    if(number>1){
      for(i<-1 until number){
        println(getRecordAtIndex(i))
      }
    }
  }

  def getRecordAtIndex(index:Int):TaxiMs={
    val mp  = new MoPoint(FMS.getID(),xDiffs(index)/1000000.0 + FMS.getLongitude(),yDiffs(index)/1000000.0 + FMS.getLatitude(),zDiffs(index)+FMS.getTimeStamp())


    val sp = (speeds(index)+FMS.getSpeed()).toShort
    val ag = (angles(index)+FMS.getAngle()).toShort
    val b1 = getA1AtIndex(index)
    val b2 = getA2AtIndex(index)
    val b3 = getA3AtIndex(index)
    val result =  new TaxiMs(mp,sp,ag,b1,b2,b3)
    result
  }

  def getA2AtIndex(index: Int): Byte ={
    val vIndex = a2s(index/8)
    val loc = index%8
    val b = 1.toByte
    val result = (vIndex>>(loc) & b).toByte
    result
  }

  def getA3AtIndex(index: Int): Byte ={
    val vIndex = a3s(index/8)
    val loc = index%8
    val b = 1.toByte
    val result = (vIndex>>(loc) & b).toByte
    result
  }

  def getA1AtIndex(index:Int): Byte ={
    val vIndex = a1s(index*2/8)
    val loc = index*2%8
    val b = 3.toByte
    val result = (vIndex>>(loc) & b).toByte
    result
  }

  // add the record at end
  def addRecord(taxiMs: TaxiMs): Long ={
    val index = number
    number = number+1

    xDiffs+= ((taxiMs.getLongitude()-FMS.getLongitude())*1000000).toInt
    yDiffs+= ((taxiMs.getLatitude()-FMS.getLatitude())*1000000).toInt
    zDiffs+= (taxiMs.getTimeStamp() - FMS.getTimeStamp()).toInt
    totalBytes = totalBytes+3*4
    if(FMS.getLongitude()<minx)
      minx = FMS.getLongitude()
    if(FMS.getLongitude() > maxx)
      maxx = FMS.getLongitude()
    if(FMS.getLatitude()<miny)
      miny = FMS.getLatitude()
    if(FMS.getLatitude() > maxy)
      maxy = FMS.getLatitude()
    maxz = taxiMs.getTimeStamp()
    speeds+= (taxiMs.getSpeed() - FMS.getSpeed()).toShort
    angles+= (taxiMs.getAngle()-FMS.getAngle()).toShort
    totalBytes = totalBytes+4

    if(index*2/8 >= a1s.length){
      a1s+=0
      totalBytes=totalBytes+1
    }


    if(index/8 >= a2s.length){
      a2s+=0
      a3s+=0
      totalBytes=totalBytes+2
    }
    a1s(index*2/8)= (a1s(index*2/8) | taxiMs.a1<<(index*2)%8).toByte
    a2s(index/8) = (a2s(index/8) | taxiMs.a2<<(index%8)).toByte
    a3s(index/8) = (a3s(index/8) | taxiMs.a3<<(index%8)).toByte

   /* if(number*2/8 >= a1s.length){
      a1s+=0
      totalBytes=totalBytes+1
    }


    if(number/8 >= a2s.length){
      a2s+=0
      a3s+=0
      totalBytes=totalBytes+2
    }
    a1s(number*2/8)= (a1s(number*2/8) | taxiMs.a1<<(number*2)%8).toByte
    a2s(number/8) = (a2s(number/8) | taxiMs.a2<<(number*2%8)).toByte
    a3s(number/8) = (a3s(number/8) | taxiMs.a3<<(number*2%8)).toByte*/
//    printTrajectory()
    totalBytes
  }

  //对所有元素构建差值
  def bulkLoadMS(MsList:List[TaxiMs]): Unit ={
    FMS = MsList(0)
    number = MsList.length
    xDiffs.clear()
    yDiffs.clear()
    zDiffs.clear()
    xDiffs.sizeHint(number)
    yDiffs.sizeHint(number)
    zDiffs.sizeHint(number)

    for(i<-0 until(number)){
      xDiffs(i) = ((MsList(i).getLongitude - FMS.getLongitude)*1000000).toInt
      yDiffs(i) = ((MsList(i).getLatitude - FMS.getLatitude)*1000000).toInt
      zDiffs(i) = (MsList(i).getTimeStamp() - FMS.getTimeStamp()).toInt
    }

    var moPoint = MsList(0)
    minx = moPoint.getLongitude
    maxx = moPoint.getLongitude
    miny = moPoint.getLatitude
    maxy = moPoint.getLatitude
    for(i<-1 until(number)){
      moPoint = MsList(i)
      if(moPoint.getLongitude < minx)
        minx = moPoint.getLongitude
      if(moPoint.getLongitude > maxx)
        maxx = moPoint.getLongitude
      if(moPoint.getLatitude < miny)
        miny = moPoint.getLatitude
      if(moPoint.getLatitude > maxy)
        maxy = moPoint.getLatitude
    }
    minz = moPoint.getTimeStamp()
    maxz = MsList(number-1).getTimeStamp()

    for(i<-0 until(number)){
      //    speeds(i)= (MsList(i+1).getSpeed() - FP.getSpeed())
      speeds(i)=  (MsList(i).getSpeed()- FMS.getSpeed()).toShort
      angles(i) =(MsList(i).getAngle() - FMS.getAngle()).toShort

      a1s(i*2/8)= (a1s(i*2/8) | MsList(i).a1<<((i*2)%8)).toByte

      a2s(i/8) = (a2s(i/8) | MsList(i).a2 <<(i%8)).toByte
      a3s(i/8) = (a3s(i/8)| MsList(i).a3<<(i%8)).toByte
    }
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput): Unit ={
    FMS.readFields(in)
    number = in.readInt()
    totalBytes = in.readLong()
    minx = in.readDouble()
    maxx = in.readDouble()
    miny = in.readDouble()
    miny = in.readDouble()
    minz =in.readLong()
    maxz = in.readLong()
    xDiffs.clear()
    yDiffs.clear()
    zDiffs.clear()
    speeds.clear()
    angles.clear()
    xDiffs.sizeHint(number)
    yDiffs.sizeHint(number)
    zDiffs.sizeHint(number)
    speeds.sizeHint(number)
    angles.sizeHint(number)

    for(i<-0 until number){
      xDiffs += in.readInt()
    }
    for(i<-0 until number){
      yDiffs += in.readInt()
    }

    for(i<-0 until number){
      zDiffs += in.readInt()
    }

    for(i<-0 until(number)){
      speeds += in.readShort()
    }


    for(i<-0 until( number)){
      angles += in.readShort()
    }

    val a1Length = number*2/8+1
    val a2Length = number/8+1
    a1s.clear()
    a2s.clear()
    a3s.clear()
    a1s.sizeHint(a1Length)
    a2s.sizeHint(a2Length)
    a3s.sizeHint(a2Length)
    for(i<-0 until(a1Length)){
      a1s += in.readByte()
    }

    for(i<-0 until(a2Length)){
      a2s += in.readByte()
    }

    for(i<-0 until(a2Length)){
      a3s += in.readByte()
    }
  }

  @throws(classOf[IOException])
  override def write(out: DataOutput): Unit = {
    FMS.write(out)
    out.writeInt(number)
    out.writeLong(totalBytes)
    out.writeDouble(minx)
    out.writeDouble(maxx)
    out.writeDouble(miny)
    out.writeDouble(maxy)
    out.writeLong(minz)
    out.writeLong(maxz)

    for(i<-0 until xDiffs.length){
      out.writeInt(xDiffs(i))
    }
    for(i<-0 until yDiffs.length){
      out.writeInt(yDiffs(i))
    }

    for(i<-0 until(zDiffs.length)){
      out.writeInt(zDiffs(i))
    }

    for(i<-0 until(speeds.length)){
      out.writeShort(speeds(i))
    }

    for(i<-0 until( angles.length)){
      out.writeShort(angles(i))
    }

    for(i<-0 until(a1s.length)){
      out.writeByte(a1s(i))
    }

    for(i<-0 until(a2s.length)){
      out.writeByte(a2s(i))
    }

    for(i<-0 until(a3s.length)){
      out.writeByte(a3s(i))
    }

  }

  override def compareTo(traj: Trajectory):Int={
    FMS.compareTo(traj.FMS)
  }

  def getTrajectoryID:Int={
    FMS.getID()
  }

  def getSpatialRange(MinX: Double,MaxX: Double,MinY: Double,MaxY: Double):Option[ArrayBuffer[Int]] = {
    getSpatialRange(new Rectangle(MinX,MaxX,MinY,MaxY))
  }

  def getSpatialRange(rect:Rectangle): Option[ArrayBuffer[Int]] ={
    var result:Option[ArrayBuffer[Int]] = None
    val arr = new ArrayBuffer[Int]
    val trajectoryMBR = new Rectangle(minx,maxx,miny,maxy)
    if(rect.contains(trajectoryMBR)){
      arr ++= (0 until number-1)
      result = Some(arr)
    }else if(trajectoryMBR.isIntersected(rect))//判断是否香蕉
    {
      val relatedRect = new Rectangle(rect.x1-FMS.getLongitude(),rect.y1-FMS.getLatitude(),rect.x2-FMS.getLongitude(),rect.y2-FMS.getLatitude())
      for(i<-0 until( xDiffs.length)){
        if(relatedRect.contains(new Point(xDiffs(i),yDiffs(i),0))) {//when x,y <0 whether it is ok?
          arr +=i
        }
      }
      result= Some(arr)
    }else{
      result = None
    }
    result
  }

  def getTimeRange(minTime :Long,maxTime:Long): Option[(Int,Int)] ={
    var result:Option[(Int,Int)] = None
    if(minTime> maxz || maxTime< minz)
      result = None
    else if(minz<minTime && maxz>minTime && maxz<maxTime){
      val bt = binarySearchUp((minTime -minz ).toInt,zDiffs)
      result = Some((bt,zDiffs.length))
      //find the first record which has a time larger than minTime
    }else if(minz> minTime && minz<maxTime && maxz > maxTime ){
      val et = binarySearchDown((maxTime-minz).toInt,zDiffs)
      result = Some((0,et))
    } else if(minz>minTime && maxz<maxTime){
      result = Some((0,zDiffs.length))
    }else {
      val bt = binarySearchUp((minTime - minz).toInt,zDiffs)
      val et = binarySearchDown((maxTime-minz).toInt,zDiffs)
      result = Some((bt,et))
    }
    result
  }

  def getSpatialTemporal(MinX: Double,MaxX: Double,MinY: Double,MaxY: Double,minTime :Long,maxTime:Long): Option[Array[TaxiMs]] ={
    var taxims : Option[Array[TaxiMs]] = None
    val timeResult = getTimeRange(minTime,maxTime)
    if(timeResult.isDefined){
      val candiResult = new ArrayBuffer[Int]()
      val relatedRect = new Rectangle(MinX-FMS.getLongitude(),MinY-FMS.getLatitude(),
                                      MaxX- FMS.getLongitude(),MaxY-FMS.getLatitude())
      for(i<-timeResult.get._1 to timeResult.get._2){
        if(relatedRect.contains(new Point(xDiffs(i),yDiffs(i),0))){
          candiResult += i
        }
      }
      if(candiResult.length==0){
        taxims=None
      }else{
        val trajSet = ArrayBuffer[TaxiMs]()
        for(i<-0 until candiResult.length){
          trajSet+= getRecordAtIndex(i)
        }
        taxims= Some(trajSet.toArray)
      }
    }else{
      taxims = None
    }
    taxims
  }

  def getSpatialTemporal(rect:Rectangle,minTime:Long,maxTime:Long):Option[Array[TaxiMs]]={
    getSpatialTemporal(rect.x1,rect.x2,rect.y1,rect.y2,minTime,maxTime)
  }

  def binarySearchUp(value: Int,arrays:ArrayBuffer[Int]): Int ={
    var start = 0
    var end = arrays.length -1
    var middle = 0
    var midValue = 0l
    while(start<=end){
      middle = (start+end)>>1
      midValue = arrays(middle)
      if(midValue==value){
        return middle
      } else if(value < midValue){
        end = middle -1
      }else{
        start = middle+1
      }
    }
    start
  }

  def binarySearchDown(value: Int,arrays:ArrayBuffer[Int]): Int ={
    var start = 0
    var end = arrays.length -1
    var middle = 0
    var midValue = 0l
    while(start<=end){
      middle = (start+end)>>1
      midValue = arrays(middle)
      if(midValue==value){
        return middle
      } else if(value < midValue){
        end = middle -1
      }else{
        start = middle+1
      }
    }
    end
  }

}

