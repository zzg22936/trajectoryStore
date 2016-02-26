package FuncTool

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.hbase.mapred.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Result
/**
  * Created by zzg on 15-12-28.
  */
object HBaseRDDRead {
  def main(args: Array[String]): Unit = {
    val conf  = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE,"test")

    val scconf = new SparkConf().setMaster("local[4]").setAppName("DataReceiver")
    val sc = new SparkContext(scconf)
    val userRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[
     Result])
    val count = userRDD.count()

    print(count)
    val key = userRDD.keys
    key.foreach(x => println(Bytes.toString(x.get())))


  }
}
