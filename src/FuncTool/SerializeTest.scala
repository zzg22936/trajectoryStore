package FuncTool

/**
  * Created by zzg on 15-12-30.
  */


object SerializeTest {

  def main(args: Array[String]): Unit = {
    //自动生存 Point 对象看如何序列花
    val a = 119.00000
    val b = 0.000001
    val c = (a-b).toInt
    val d = c <<20
    println("c = "+c)
    println("d = "+d)
    println("c*10^6 = "+(c*1000000))
   /* val mo1 = new TaxiMs(new MoPoint(11,132.23222,34.4533,7866),54,45,1,1,1)
    val mo2 = new TaxiMs(new MoPoint(56,97.8642,22.84665,343),43,34,1,1,1)
    println(mo1.toString)
    println(mo2.toString)
    /*val text =  new Text()
    mo1.toText(text)
    println(text)
    mo2.toText(text)
    println(text)*/
    val conf = new Configuration()
    val fs =  FileSystem.get(conf)
    val path = new Path("~/user/zzg/seq")
    val writer = SequenceFile.createWriter(conf,Writer.file(path),Writer.keyClass(classOf[TaxiMs]),Writer.valueClass(classOf[NullWritable]))
//    val write = new Writer(fs,conf,path,classOf[TaxiMs],classOf[NullWritable])
    println(writer.getLength)
    writer.append(mo1,NullWritable.get())
    println(writer.getLength)
    writer.append(mo2,NullWritable.get())
    println(writer.getLength)
    IOUtils.closeStream(writer)


    val read = new Reader(conf,Reader.file(path))
    val taxims = new TaxiMs()
    while(read.next(taxims)){
      println(taxims.toString)
    }*/
  }

}
