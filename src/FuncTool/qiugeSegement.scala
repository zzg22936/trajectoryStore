package FuncTool

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{TextInputFormat, TextOutputFormat}

//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zzg on 15-12-30.
  */
object qiugeSegement {
  def main(args: Array[String]): Unit = {
  val rec =  "20131001000159,001140,116.406975,39.988724,45.000000,264.000000,1,1,0,20131001000159"
    println(rec.charAt(21))
    println(rec.substring(15,21))
    println(rec.substring(0,14))
    println(rec.substring(22))
    println(rec.substring(22,rec.length-15))
    println(Integer.toHexString(255))
 //   getSegements()
 /*   val beginTime ="20101009075959"
    println("20101009075959">"20101009075900")
    println("20101009065959">"20101009075900")
    filtSegments*/
  }

  def search(): Unit ={

  }

  def filtSegments(): Unit ={
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[4]")
    conf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LongWritable],classOf[Text]))
    val sc = new SparkContext(conf)

    val records =sc.hadoopFile("hdfs://localhost:9000/user/zzg/2013-10-09",classOf[TextInputFormat],classOf[LongWritable],classOf[Text])
//    val records = sc.textFile("hdfs://localhost:9000/user/zzg/2013-10-09")
    val beginTime ="20131009075959"
    val brbegin =sc.broadcast(beginTime)
    val endTime =  "20131009190000"
    val brend = sc.broadcast(endTime)
    implicit val KeyOrdering = new Ordering [String]{
      override def compare(x: String, y: String): Int = {
        x.compareTo(y)
      }
    }
    val result =  records.map(x=>{
     val tokens = x._2.toString.split("\t")
      (tokens(0),x._2.toString.substring(tokens(0).length+"\t".length))
    })
      .filter(pair =>{
      pair._1>brbegin.value && pair._1<brend.value
    })
      .sortByKey(true)
      .saveAsHadoopFile("/home/zzg/2013-10-09",classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]])
      /*.filter(keyValue =>{
     val tey = keyValue._2.toString.split("\t")
      tey(0).compareTo(beginTime)>0 &&   tey(0).compareTo(endTime)<0
    }.sortByKey(true)
    .saveAsHadoopFile("/home/zzg/2013-10-09/8-16",classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]])*/

  }

  def getSegements(): Unit ={

    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[4]")
    conf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LongWritable],classOf[Text]))
    val sc = new SparkContext(conf)

  //  val records = sc.textFile("hdfs://localhost:9000/user/zzg/beijing_taxi/2013-10-09/Info-[1-2]*")
  val records = sc.textFile("hdfs://localhost:9000/user/zzg/beijing_taxi/2013-10-09/Info-[1-2]*")
/*   val segments=   records.map(x=> {
          var id = ""
          if(x.charAt(21).equals(",")){
            id=x.substring(15,21) //6 bytes id
          }else{
            id = x.substring(15,20) //5 bytes id
          }
          val time = x.substring(0,14)
          (id, (time, x.substring(22,x.length-15)))
        }).groupByKey(13)
          .flatMap(rc =>{
            val sorted =  rc._2.toList.sortWith(_._1<_._1)
            val tuples = new Array[(((String,String),(String,String)),String)](sorted.length-1)
            val newKeyValue = new Array[(String,String)](sorted.length -1)
            for (i<-0 until sorted.length-1){
              tuples(i) = ((sorted(i),sorted(i+1)),rc._1 )
          //    newKeyValue(i) = (sorted(i+1)._1,(sorted(i+1)._2 +"\t" sorted(i)._ + "\t" + sorted(i)._2))
            }
            tuples
          })

     implicit val KeyOrdering = new Ordering [((String,String),(String,String),String)]{
      override def compare(x: ((String,String),(String,String),String), y: ((String,String),(String,String),String)): Int = {
        y._2._1.compareTo(x._2._1)
      }
    }*/

  //   segments.sortByKey().saveAsTextFile("/home/zzg/sortedSegements")
//    segments.sortBy(KeyOrdering)

    val segments=  records.map(x=> {
      var id = ""
      if(x.charAt(21).equals(",")){
        id=x.substring(15,21) //6 bytes id
      }else{
        id = x.substring(15,20) //5 bytes id
      }
      val time = x.substring(0,14)
      (id, (time, x.substring(22,x.length-15)))
    }).groupByKey(24)
      .flatMap(rc =>{
        val sorted =  rc._2.toList.sortWith(_._1<_._1)
        val newKeyValue = new Array[(Text,Text)](sorted.length -1)
        for (i<-0 until sorted.length-1){
          newKeyValue(i) = (new Text(sorted(i+1)._1) , new Text(sorted(i+1)._2 + "\t"+ sorted(i)._1 + "\t" + sorted(i)._2))
        //      newKeyValue(i) = (sorted(i+1)._1,(sorted(i+1)._2 +"\t"+ sorted(i)._1 + "\t" + sorted(i)._2))
        }
        newKeyValue
      })
    implicit val KeyOrdering = new Ordering [Text]{
      override def compare(x: Text, y: Text): Int = {
        x.toString.compareTo(y.toString)

      }
    }
    segments.sortByKey().saveAsHadoopFile("/home/zzg/2013-10-09",classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]])

  //  segments.saveAsTextFile("/home/zzg/sortedSegements")
  //  segments.saveAsHadoopFile("/home/zzg/sortedHadoopSegements",classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]])
   // segments.sortByKey().saveAsTextFile("/home/zzg/sortedSegements")
    //    segments.sortBy(KeyOrdering)

  }
}
