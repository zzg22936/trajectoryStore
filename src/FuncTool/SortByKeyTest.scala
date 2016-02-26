package FuncTool

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zzg on 15-12-30.
  */
object SortByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[4]")
    conf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LongWritable],classOf[Text]))
    val sc = new SparkContext(conf)

    val b = sc.parallelize(List(3,1,9,12,4),2)
    val a = sc.parallelize(List("wyp", "iteblog", "com", "397090770", "test"), 2)
    val c = b.zip(a)
    c.foreach(println)
    println("sorted")
    val d = c.sortByKey().collect()
    d.foreach(println)
    implicit val sortIntegersByString = new Ordering[Int]{
       override def compare(a: Int, b: Int) =
         a.toString.compare(b.toString)
    }
    val e = c.sortByKey().collect()
    e.foreach(println)

  }

}
