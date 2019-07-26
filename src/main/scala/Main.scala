import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {

    val arenaPlayZone = " 8_"
    val initTourPosition = "initPosition uids"
    val finishTour = "ScoreHisCmd"
//    val tourPlayZone = " 8_"
//    val finishTourPlay = "ScoreHisCmd"

    Class.forName("com.mysql.jdbc.Driver").newInstance()
    println("done!")


    val ss: SparkSession = SparkSession.builder.appName("api1").config("spark.master", "local").getOrCreate()
    val sc = ss.sparkContext

    import ss.implicits._
    import DB._

//    val dateCfg = ss.readFromSpark("date").select("start", "end", "last")
//      .as[DateCfg]
//      .take(1)
//      .headOption
//      .getOrElse(DateCfg.init)

    val fileGroup = getFileList(ss, "data/", "2019-06-24-00", "2019-07-30-00", "2019-06-24-00")
    fileGroup.foreach{files => {
//      val firstFileName = files(0).getName.split("[.]")(2).replace("-", "")
      val logs = files.sortBy(_.getName).map(f => sc.textFile(f.toUri.getPath))

      val sessions = logs.map(_.filter(l => (l.contains(arenaPlayZone) && l.contains(finishTour))))
        .fold(sc.emptyRDD[String])((o1, o2) => o1 ++ o2)
        .map(l => parseArena(l)).sortBy(-_._1)
      sessions.coalesce(1, true).saveAsTextFile("dinh1")
//      }
    }}
  }

  def getFileList(ss: SparkSession, p: String, startDate: String, endDate: String, lastDate: String): List[Array[Path]] = {
    val path = new Path(p)
    val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
    val fileList = fs.listStatus(path).filter(_.isFile).map(_.getPath)
      .filter(n => n.getName >= s"smartfox.log.$startDate" && n.getName > s"smartfox.log.$lastDate" && n.getName <= s"smartfox.log.$endDate")
      .sortBy(_.getName)

    fileList.grouped(8).toList
  }

  def getFileList1(ss: SparkSession, p: String): List[Array[Path]] = {
    val path = new Path(p)
    val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
    val fileList = fs.listStatus(path).filter(_.isFile).map(_.getPath)
      .filter(n => n.getName.startsWith("api.2"))
      .sortBy(_.getName)

    fileList.grouped(18).toList
  }

  def parseLine(line: String): Line = {

    val x = line.split(" [|] ")
    val d = DateTime.parse(s"${x(0)} ${x(1)}", DateTimeFormat.forPattern("dd MMM yyyy HH:mm:ss,SSS"))
    val l = x.last.split(", ")
    val tpe = if (l(0).contains("login")) 1 else if (l(0).contains("disconnected")) 2 else 3
    val userIdTmp = l(1).split(": ").last
    val uid = if (userIdTmp.forall(_.isDigit))  userIdTmp.toInt else 0
    val sId = l(2).split(": ").last.toInt
    val session = l(4).split(" ")(1)
    val sessionLength = if (!line.contains("login")) l(5).split(": ").last.toInt else 0
    Line(tpe, if (tpe == 1) d.getMillis else 0, if (tpe != 1) d.getMillis else 0, uid, sId, session, sessionLength)
  }

  def parseLine1(line: String): PayLog = {
    if (line.contains("sd.pay.ussdcard.PayCardByUssdActor")) {

      //2019-04-25 07:46:32,274 | INFO | sd.pay.ussdcard.PayCardByUssdActor | SDPaySuccessTopic vnd = 20000 && request = *100*110704803961405#;5265895;viettel_8984048000044966354_0;1556153186
      val arr = line.split(" ")
      println(line)
      val amount = arr.slice(3, arr.length - 1).filter(f=> f.contains("000")).head.toInt
      val infos = arr.last.split(";") // *100*110704803961405#;5265895;viettel_8984048000044966354_0;1556153186
      val pin = infos.head.replace("*100*", "").replace("#", "")
      val df = DateTime.parse(s"${arr(0)} ${arr(1)}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS"))
      PayLog(pin, infos(2).split("_").head, amount, (df.getMillis / 1000).toInt, line, "")
    } else {
      val arr = line.split("[|]")
      val d = arr.last.split("payCardSimGw").last.replace(" ", "").split("-") // 50000-Some(10003852204345)-411547804831118!

      val df = DateTime.parse(s"${arr(0).trim}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS"))

      PayLog(d.last.replace("!", ""), "viettel?", d(0).toInt, (df.getMillis / 1000).toInt, line, d(1).replace("Some(", "").replace(")", ""))
    }
  }


  def parseArena(line: String) = {
//    val x = s"""-> ScoreHisCmd 8_0_39 1402548,3152806,5528,1109480,4596217 : {"c":[["traibepzai","Nhất Cửu Thất Cửu","LienPhuong","phongpham79"],[6,-2,-2,-2],"l1ptnogb_1prb",[15,-5,-5,-5],"l1ptnolw_1prb",[-5,-5,15,-5],"l1ptnoqp_1prb",[-12,-12,-12,36],"l1ptnot7_1prb",[9,-3,-3,-3],"l1ptnoxl_1prb",[-4,-4,12,-4],"l1ptnp10_1prb",[-11,33,-11,-11],"l1ptnp58_1prb",[-3,9,-3,-3],"l1ptnp8v_1prb",[-5,15,-5,-5],"l1ptnpeg_1prb",[-3,9,-3,-3],"l1ptnpi7_1prb",[-6,18,-6,-6],"l1ptnpnn_1prb",[45,-15,-15,-15],"l1ptnpqh_1prb",[15,-5,-5,-5],"l1ptnpvh_1prb",[-4,-4,-4,12],"l1ptnpzc_1prb",[-16,-16,-16,48],"l1ptnq4s_1prb",[-51,17,17,17],"l1ptnq8c_1prb",[45,-15,-15,-15],"l1ptnqdz_1prb",[-2,-2,-2,6],"l1ptnqhw_1prb",[-5,15,-5,-5],"l1ptnqlq_1prb",[12,-4,-4,-4],"l1ptnqph_1prb",[-16,48,-16,-16],"l1ptnqum_1prb",[-3,-3,-3,9],"l1ptnqzw_1prb",[-4,-4,12,-4],"l1ptnr53_1prb",[-5,15,-5,-5],"l1ptnrb6_1prb",[-51,17,17,17],"l1ptnrer_1prb",[45,-15,-15,-15],"l1ptnrjf_1prb",[-3,9,-3,-3],"l1ptnrmy_1prb",[-3,-3,-3,9],"l1ptnrqb_1prb",[-16,-16,-16,48],"l1ptnrv9_1prb",[-2,-2,6,-2],"l1ptns09_1prb",[-38,70,-98,66]],"u":[1402548,4596217,3152806,5528],"f":2,"o":[3152806,1402548,5528,4596217]}"""
    val y = line.split(" [|] ").last.split(s""" : """)
    val z = y(0)
    val z1 = y(1)
    val room = z.split(" ")(2)
    //val uid = z.split(" ")(3).split(",").map(_.toInt)
    val z2 = z1.split(""","u":""")
    val lastScore = z2.head.split("\",").last.replace("[","").replace("]", "").split(",").map(_.toInt)
    val uid = z2.last.split("\"o\":").last.replace("[","").replace("]","").replace("}","").split(",").map(_.toInt)
    val username =  z2.head.split("\\],\\[").head.replace("{\"c\":[[", "").split(",")
    val mapScore = lastScore.zipWithIndex.map{case (value, index) => (value, uid(index), username(index))}.maxBy(_._1)
    mapScore
    // uid
  }

  def parseTour(line: String) = {
    if (line.contains("initPosition uids")) {
      parseInitPosition(line)
    } else {
      parseFinish(line)
    }
  }

  def parseInitPosition(line: String) = {
    val arrs = line.split(" [|] ")
    val time = parseDate(s"${arrs(0).trim} ${arrs(1).trim}")
    val room = arrs.last.trim.split(" ").head
    val uids = arrs.last.trim.split(" ").last.split(",").map(_.toInt)
    val uid2Str = uids.mkString(",")
    val key = s"${room}_${uid2Str}"
    TourData(1, uids, time.toInt, 0, key)
  }

  def parseFinish(line: String) = {
    val data = line.split("ScoreHisCmd").last.trim
    val h = line.split("ScoreHisCmd").head.split(" [|] ")
    val time = parseDate(s"${h(0).trim} ${h(1).trim}")
    val ds = data.split(" : ")
    val d2 = ds.last.split(",\"u\":")
    val score = d2.head.split("\",\\[").last.replace("]", "").split(",").map(_.toInt)
    val uids = d2.last.split(",\"o\":\\[").last.replace("]}", "").split(",").map(_.toInt)
    val room = ds.head.split(" ").head
    val uid2Str = uids.mkString(",")
    val key = s"${room}_${uid2Str}"
    TourData(1, uids, 0, time.toInt, key)
  }

  def parseDate(d: String) = {
    DateTime.parse(d, DateTimeFormat.forPattern("dd MMM yyyy HH:mm:ss,SSS")).getMillis/1000
  }
}

case class TourData(tpe:Int, uids:Seq[Int], startTime: Int, endTime: Int, key: String)
case class Line(tpe: Int, startTime: Long, endTime: Long, uid: Int, smfId: Int, smfSession: String, sessionLength: Int) {
  def key = s"$uid-$smfId-$smfSession"
}

case class PayLog(pin: String, provider: String, amount: Int, time: Int, data: String, seri: String)

case class ArenaUser(uids: Seq[Int])

case class DateCfg(start: String, end: String, last: String)

object DateCfg {
  val format = new SimpleDateFormat("yyyy-MM-dd-HH")
  def init = {
    val current = format.format(new Date())
    DateCfg("2018-08-12-00", current, "2018-08-18-00")
  }
}