package FlightDataAnalysis

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.log4j.{Level, Logger}
import com.typesafe.config.ConfigFactory
/**
  * Created by Piyush Tripathi on 3/17/2017.
  */
object LoadTest {

  val city = ConfigFactory.load().getString("my.value")

  def main(args: Array[String]): Unit = {


    /*------------------- start: function getWeek()----------------*/

    def getWeek(year:Int,month:Int,day:Int): Int ={

      var i=0
      var ttlinmonth =0

      for(i<- 1 to month-1){
        ttlinmonth += getdaysinMonth(i,year)
      }
      (ttlinmonth+day)/7 + 1
    }

    def getdaysinMonth(month: Int, year:Int): Int ={
      if(month == 4 || month == 6 || month == 9 || month == 11) 30
      else if(month == 2 && year%4 != 0) 28
      else if(month == 2) 29
      else 31
    }

    def getdaysinYear(year: Int): Int ={
      if(year % 4 == 0) 366 else 365
    }

    /*------------------- end: function getWeek()----------------*/


    val conf = new SparkConf().setMaster("local[*]").setAppName("My LoadTest App")
    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)

    val flightdata = sc.textFile("C:\\Users\\Piyush\\Downloads\\airports.csv")

    val header = flightdata.first()
    val nonheader = flightdata.filter(row=>row!=header)

    //val delaydata = sc.textFile("D:\\Flight_input\\200*").filter(row=>row(1)!="\\+s").filter(x=>x.contains(city)).cache()
    val delaydata = sc.textFile("src\\main\\input\\200*").filter(row=>row(1)!="\\+s").filter(x=>x.contains(city)).cache()

    case class YearlyData(
                           year: Int,
                           carrier: String,
                           arrDelay:Int,
                           depDelay:Int,
                           origin:String,
                           dest:String,
                           week:Int
                         )


    val ArrDelayRDD = delaydata.map{line=>
      val cols = line.split(",")
      YearlyData(cols(0).toInt,cols(8),0,if(cols(15)=="NA")0 else cols(15).toInt,cols(16),"NI",getWeek(cols(0).toInt,cols(1).toInt,cols(2).toInt))
    }.filter(x=>x.origin==city).filter(x=>x.depDelay>0)

    val DepDelayRDD = delaydata.map{line=>
      val cols = line.split(",")
      YearlyData(cols(0).toInt,cols(8),if(cols(14)=="NA")0 else cols(14).toInt,0,"NI",cols(17),getWeek(cols(0).toInt,cols(1).toInt,cols(2).toInt))
    }.filter(x=>x.dest==city).filter(x=>x.arrDelay>0)

    val ttlDepDelayWeekly = DepDelayRDD.map(x=>((x.year,x.carrier,x.week),x.arrDelay)).reduceByKey((x,y)=>x+y).cache
    val ttlArrDelayWeekly = ArrDelayRDD.map(x=>((x.year,x.carrier,x.week),x.depDelay)).reduceByKey((x,y)=>x+y).cache

    ttlDepDelayWeekly.join(ttlArrDelayWeekly).map(x=>(x._1,x._2._1.toInt + x._2._2.toInt)).sortByKey(true).saveAsTextFile("src\\main\\output")

  }
}
