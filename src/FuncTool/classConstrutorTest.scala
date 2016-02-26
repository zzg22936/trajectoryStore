package FuncTool

/**
  * Created by zzg on 16-1-24.
  */
class classConstrutorTest (a :Int){

  val num = new Array[Int](10)
  num(0) = a

  def print(): Unit ={
    num.foreach(println)
  }

}


object classConstrutorTest{
  def main (args: Array[String]) {

    val a= 124.0001
    val b= 120.2345
    val c = (a-b)
    println(c.toInt)
    println((c*1000000).toInt)

  }

  def testTrajectory: Unit ={
    val fb = new classConstrutorTest(1)
    fb.print()

    val a = 9
    println(a/8+1)
    println(11*2 /8+1)
    val bs = new Array[Byte]( 5)
    bs(0)=1
    bs(1)=2
    bs(2)=0
    bs(3)=2
    bs(4)=1
    val barray = new Array[Byte](5*2/8+1)
    for(i<-0 until 5){
      println("i*2/8 = "+(i*2/8))
      println("bs(i) = "+ bs(i))
      println("bs(i)<<((i*2)%8) = "+  (bs(i)<<((i*2)%8)))
      println("(barray(i*2/8) & bs(i)<<((i*2)%8)).toByte = "+(barray(i*2/8) & bs(i)<<((i*2)%8)).toByte)
      barray(i*2/8) = (barray(i*2/8) | bs(i)<<((i*2)%8)).toByte
    }
    barray.foreach(println)

  }

}