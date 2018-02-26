import org.apache.spark.{SparkContext,SparkConf}
import java.text.SimpleDateFormat
case class customer(custid: Int,name:String,street:Int,city:String,state:String,zip:Int)
case class shoping(timestamp: Long,custid:Int,salesprice:Float)

object ImplementingJoinOnTwoFiles  extends App{
  val conf = new SparkConf()

  conf.setAppName("joiningOfTwoFiles")
  conf.setMaster("local");
  val sc = new SparkContext(conf)
  val rdd1 = sc.textFile("file1").map(_.split("#"))
  val rdd2 = sc.textFile("file2").map(_.split("#"))
  val cust_record1 = rdd1.map(x =>(x(0).toInt,customer(x(0).toInt,x(1),x(2).toInt,x(3),x(4),x(5).toInt)))
  val cust_record2 = rdd2.map(x =>(x(1).toInt,shoping(x(0).toLong,x(1).toInt,x(2).toFloat)))
  val result = cust_record1.join(cust_record2)
  result.collect.foreach(println)

  def convertingTimestamptoDate(): String = {


    val ts = 1480665459 * 1000L
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val date = df.format(ts)
    date
  }
}
