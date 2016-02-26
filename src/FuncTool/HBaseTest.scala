package FuncTool



import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zzg on 15-12-25.
  */
object HBaseTest {
  def main(args: Array[String]): Unit = {

  val a = 0x11ff
    val b = 1l
    val c = 0xff11

    val k = 12.023
    val kb = Bytes.toBytes(k)
    kb.foreach(x=>print(x+" "))

    /*val bytes = Bytes.toBytes(a) ++ Bytes.toBytes(b) ++ Bytes.toBytes(c)
    bytes.foreach(x=>print(x+" "))

    println()
    val s = bytes.slice(0,4)
    s.foreach(print)
    val ra = Bytes.toInt(s)
    println()
    println(ra)

    val rb = Bytes.toInt(bytes.slice(4,12))
    println(rb)
    val rc = Bytes.toInt(bytes.slice(12,16))

    println(rc)
*/
  }

  def writeToText(): Unit ={
    //定义 HBase 的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "localhost")

    //指定输出格式和输出表名
    val jobConf = new JobConf(conf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"user")

    val rawData = List((1,"lilei",14), (2,"hanmei",18), (3,"someone",38))

    val scconf = new SparkConf().setMaster("local[4]").setAppName("DataReceiver")
    val sc = new SparkContext(scconf)
    val localData = sc.parallelize(rawData).map(convert)
    localData.saveAsHadoopDataset(jobConf)
  }

  def convert(triple: (Int, String, Int)) = {
    val p = new Put(Bytes.toBytes(triple._1))
    p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"),Bytes.toBytes(triple._2))
    p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("age"),Bytes.toBytes(triple._3))
    (new ImmutableBytesWritable, p)
  }

  def connectionTest(): Unit ={

    //   val conf = new SparkConf().setMaster("local[4]").setAppName("DataReceiver")
    // org.apache.hadoop.hbase.client.RpcRetryingCallerFactory
    val hBaseConf  = HBaseConfiguration.create()
    //集群环境下 要使每个worker node访问不了hbase内容。原因我们之前的例子里没有指定hbase的地址信息
    hBaseConf.set("hbase.zookeeper.quorum","localhost")//指定ip地址
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181") // zookeeper的端口号
    hBaseConf.addResource(new Path("/home/zzg/Softwares/hbase/conf/hbase-site.xml"))
    //   val sc = new SparkContext(conf)

    //    val broadCastHBaseConf = sc.broadcast(new SerializableWritable(hBaseConf) )
    /* val a =sc.parallelize(1 to 9,3)

     a.foreach(x=>println(x))
     a.foreach(x=>println(x))*/

    val methods = new org.apache.hadoop.hbase.util.Addressing().getClass.getMethods
    methods.foreach(method=>println(method.getName))


    try{
      val connection = ConnectionFactory.createConnection(hBaseConf)

      val table = connection.getTable(TableName.valueOf("test"))

      val scan = new Scan()
      val resultScanner = table.getScanner(scan)
      var res = resultScanner.next()
      while(res!=null){
        val rowBytes = res.getRow
        val key = Bytes.toString(rowBytes)
        println("ok")
        println(key)
        res = resultScanner.next()
      }
    }catch {
      case a: Exception=> a.printStackTrace()
    }
  }

}
