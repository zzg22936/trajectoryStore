package edu.ecnu.idse.TrajStore.Taxi

import java.io.{DataInput, DataOutput, IOException}
import java.text.SimpleDateFormat

import edu.ecnu.idse.TrajStore.core.{CellInfo, MoPoint}
import edu.ecnu.idse.TrajStore.util.SpatialUtilFuncs
import org.apache.hadoop.io.WritableComparable

/**
  * Created by zzg on 16-1-29.
  */
class TaxiMs(var moPoint: MoPoint, var speed:Short,var angle:Short, var a1:Byte,var a2:Byte, var a3:Byte) extends  WritableComparable[TaxiMs] {

  def this(){
    this(new MoPoint(),0,0,0,0,0)
  }

  @throws(classOf[IOException])
  override def write(out: DataOutput) {
    moPoint.write(out)
    out.writeShort(speed)
    out.writeShort(angle)
    out.writeByte(a1)
    out.writeByte(a2)
    out.writeByte(a3)
  }

  @throws(classOf[IOException])
  def readFields(in: DataInput) {
    moPoint.readFields(in)
    speed = in.readShort()
    angle = in.readShort()
    a1 = in.readByte()
    a2 = in.readByte()
    a3 = in.readByte()
  }

  //重写超（父）类的方法shi,不要 overwrite
  override def equals(obj: Any): Boolean = {
    if(obj.isInstanceOf[TaxiMs]){
      val r2: TaxiMs = obj.asInstanceOf[TaxiMs]
      this.moPoint.equals(r2.moPoint)
    }else{
      false
    }
  }

  /*  def toText(text: Text): Text = {
      TextSerializerHelper.serializeDouble(speed, text, ',')
      TextSerializerHelper.serializeInt(a1,text,',')
      TextSerializerHelper.serializeInt(a2,text,',')
      TextSerializerHelper.serializeInt(a3,text,',')
      moPoint.toText(text)
      return text
    }

    def fromText(text: Text) {
      speed = TextSerializerHelper.consumeDouble(text, ',')
      a1 = TextSerializerHelper.consumeInt(text, ',')
      a2 = TextSerializerHelper.consumeInt(text, ',')
      a3 = TextSerializerHelper.consumeInt(text, ',')
      moPoint.fromText(text)
    }*/

  override def compareTo(taxiMs: TaxiMs):Int={
    return moPoint.compareTo(taxiMs.moPoint)
  }

  def getTimeStamp():Long = {
    moPoint.getTimeStamp
  }

  def getSpeed():Short = {
    speed
  }

  def getAngle():Short = {
    angle
  }

  override def toString(): String ={
    return moPoint.toString+","+speed+","+angle+","+ a1 +","+ a2+","+ a3
  }

  def getLongitude():Double={
    moPoint.getLongitude
  }

  def getLatitude():Double={
    moPoint.getLatitude
  }

  def getLocation():(Double,Double)={
    return (moPoint.getLongitude,moPoint.getLatitude)
  }

  def getID(): Int ={
    return moPoint.getID
  }

  //20131009000007,001140,116.435394,39.959473,0.000000,98.000000,2,1,0,20131009000011
  def parseMessage(str: String,index :Array[CellInfo],beginTime:Long,carMap:collection.Map[java.lang.String,Int]):(Int,TaxiMs)={
    val tmp = str.split(",")
    // println(tmp.apply(0))
    val localSD = new SimpleDateFormat("yyyyMMddHHmmss")
    //20131001000159,001140,116.406975,39.988724,45.000000,264.000000,1,1,0,20131001000159
    val tNow = localSD.parse(tmp.apply(0)).getTime
    val time1 = (tNow - beginTime) / 1000
    val tEnd = localSD.parse(tmp.apply(9)).getTime
    val time2 = ((tEnd - beginTime) / 1000)

    val cid = carMap.get(tmp.apply(1)).get
    val log = tmp(2).toFloat
    val lat = tmp(3).toFloat
    val region = SpatialUtilFuncs.getLocatedRegion(log,lat,index)
    val speed = tmp(4).toFloat.toShort
    val angle = tmp(5).toFloat.toShort
    val a1 = tmp(6).toByte
    val a2 = tmp(7).toByte
    val a3= tmp(8).toByte
    val hashCarID = cid%7
    val split = (region.toLong <<32) + hashCarID
    (region,new TaxiMs(new MoPoint(cid,log,lat,time1),speed,angle,a1,a2,a3))
    /*
          val hashCarID = carID%7
          val split = (region.toLong <<32) + hashCarID          //region:hashCarID
          new TaxiMessage(split, temp(0).toLong,carID, longitude, latitude, speed)
          //split time           id      logi      lat      spped
    */
  }
}

object TaxiMs{
  def parseMessage(str: String,index :Array[CellInfo],beginTime:Long,carMap:collection.Map[java.lang.String,Int]):(Int,TaxiMs)={
    val tmp = str.split(",")
    // println(tmp.apply(0))
    val localSD = new SimpleDateFormat("yyyyMMddHHmmss")
    //20131001000159,001140,116.406975,39.988724,45.000000,264.000000,1,1,0,20131001000159
    val tNow = localSD.parse(tmp.apply(0)).getTime
    val time1 = (tNow - beginTime) / 1000
    val tEnd = localSD.parse(tmp.apply(9)).getTime
    val time2 = ((tEnd - beginTime) / 1000)

    val cid = carMap.get(tmp.apply(1)).get
    val log = tmp(2).toFloat
    val lat = tmp(3).toFloat
    val region = SpatialUtilFuncs.getLocatedRegion(log,lat,index)
    val speed = tmp(4).toFloat.toShort
    val angle = tmp(5).toFloat.toShort
    val a1 = tmp(6).toByte
    val a2 = tmp(7).toByte
    val a3= tmp(8).toByte
    val hashCarID = cid%7
    val split = (region.toLong <<32) + hashCarID
    (region,new TaxiMs(new MoPoint(cid,log,lat,time1),speed,angle,a1,a2,a3))
    /*
          val hashCarID = carID%7
          val split = (region.toLong <<32) + hashCarID          //region:hashCarID
          new TaxiMessage(split, temp(0).toLong,carID, longitude, latitude, speed)
          //split time           id      logi      lat      spped
    */
  }

}
