import java.util.NoSuchElementException

import org.apache.log4j.{Logger, Level}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import stringutil.Unicode

import scala.util.matching.Regex

/**
  * Created by NK on 2017. 2. 9..
  */
object MediaAnalyze {
  type Triple = (String, String, String)

  val NTRIPLE_PATH = "data/new_n3/AmusementPark_n3_thumb,data/new_n3/Bicycle_stunt_n3_thumb,data/new_n3/BirthdayParty_n3_thumb,data/new_n3/Camping_n3_thumb,data/new_n3/Climbing_n3_thumb,data/new_n3/Cook_n3_thumb,data/new_n3/Cute_festival_n3_thumb,data/new_n3/Entrance_ceremony_n3_thumb,data/new_n3/First_birthday_n3_thumb,data/new_n3/Fishing_n3_thumb,data/new_n3/Golf_n3_thumb,data/new_n3/GraduationCeremony_n3_thumb,data/new_n3/Makeup_n3_thumb,data/new_n3/Pet_n3_thumb,data/new_n3/Tod_n3_thumb,data/new_n3/Wedding_n3_thumb"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Media Analyze").setMaster("local[*]")
    val sc = new SparkContext(conf)
    setLogLevel(Level.WARN)

    val inputTripleRDD = sc.textFile(NTRIPLE_PATH).map(Unicode.decode).mapPartitions(parseNTriple, true)

    /**
      * 예제코드.
      */
    printEvents(inputTripleRDD)

    
  }

  /**
    * 전체 미디어 데이터의 Event Type을 출력해주는 코드.
    * @param tripleRDD
    */
  def printEvents(tripleRDD:RDD[Triple]) = {
    tripleRDD.filter{spo => spo._1.contains("Video") && spo._2.contains("#type") && spo._3.contains("Event")}
      .map{spo => spo._3}.distinct().foreach(println)
  }

  /**
    * Spark 실행시 Log 표시 레벨을 정할 수 있음.
    * @param level
    */
  def setLogLevel(level: Level): Unit = {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
  }
  /**
    * Line 문자열을 Triple 형태로 바꿔주는 함수.
    *
    * @param lines
    * @return
    */
  def parseNTriple(lines: Iterator[String]) = {
    val TripleParser = new Regex("(<[^\\s]*>)|(_:[^\\s]*)|(\".*\")")
    for (line <- lines) yield {
      try{
        val tokens = TripleParser.findAllIn(line)
        val (s, p, o) = (tokens.next(), tokens.next(), tokens.next())
        (s, p, o)

      }catch {
        case nse: NoSuchElementException => {
          ("ERROR", "ERROR", "ERROR")
        }
      }
    }
  }
}
