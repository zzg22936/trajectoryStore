package edu.ecnu.idse.TrajStore.util

/**
  * Created by zzg on 15-12-21.
  */
object IndexType extends Enumeration{
  type IndexType = Value
  val GRIDINDEX,KDTREEINDEX, QUARTREEINDEX, RTREEINDEX = Value
}

object TestIndexType{
  def main(args: Array[String]): Unit = {
    IndexType.values.foreach(v => println(v,v.id))

  }
}
