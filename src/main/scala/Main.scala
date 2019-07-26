import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.text.SimpleDateFormat
import java.util.{Date}

import org.apache.spark.sql.{SaveMode, SparkSession}

object Main {
  val vipSumupStr = s" afterSumup 5_"
  val thiHuongSumup = s" afterSumup 6_"
  val thiHoiSumup = s" afterSumup 7_"
  val thiDinhSumup = s" afterSumup 8_"
  val dauTruongSumup = s" afterSumup 9_"
  val serverName = scala.util.Properties.envOrElse("SPARK_SERVER", "f1")

  def main(args: Array[String]): Unit = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    println("start!")

    val ss: SparkSession = SparkSession.builder.appName("api1").config("spark.master", "local").getOrCreate()
    val sc = ss.sparkContext

    import ss.implicits._
    import DB._

    val dateCfgList = ss.readFromSpark("date").select("server","start", "end", "last")
      .as[DateCfg]

    val dateCfg = dateCfgList.filter($"server" === serverName)
      .take(1)
      .headOption
      .getOrElse(DateCfg.init)
    val fileGroup = getFileList(ss, "/data/", dateCfg.start, dateCfg.end, dateCfg.last)


    fileGroup.foreach(println(_))
    fileGroup.foreach{files => {
      val logs = files.sortBy(_.getName).map(f => sc.textFile(f.toUri.getPath))

      val sessions = logs.map(_.filter(l => l.contains(vipSumupStr) || l.contains(thiHuongSumup) || l.contains(thiHoiSumup) || l.contains(thiDinhSumup)
      || l.contains(dauTruongSumup)))
        .fold(sc.emptyRDD[String])((o1, o2) => o1 ++ o2)
          .map(l => parseAfterSumup(l))

      sessions.filter(_.tpe == 1).toDS().saveToSpark(s"vip_session", SaveMode.Append)
      sessions.filter(_.tpe == 2).toDS().saveToSpark(s"huong_session", SaveMode.Append)
      sessions.filter(_.tpe == 3).toDS().saveToSpark(s"hoi_session", SaveMode.Append)
      sessions.filter(_.tpe == 4).toDS().saveToSpark(s"dinh_session", SaveMode.Append)
      sessions.filter(_.tpe == 5).toDS().saveToSpark(s"arena_session", SaveMode.Append)

      files.lastOption.foreach{p => {
        val name = p.getName.replace("smartfox.log.", "")
        dateCfgList.collect().toSeq.map(i => if (i.server == serverName) DateCfg(i.start, i.end, name, i.server) else i)
          .toDS.saveToSpark("date", SaveMode.Overwrite)
      }}
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

  def parseAfterSumup(line: String): AfterSumup = {
    val data = line.split("afterSumup")
    val last = data.last.trim.split(" ")
    val uids = last(1).split(",").map(_.toInt)
    val d1 = line.split(" [|] ")
    val time = parseDate(s"${d1(0).trim} ${d1(1).trim}").toInt
    val tpe = if (line.contains(vipSumupStr)) {
      1
    } else if (line.contains(thiHuongSumup)) {
      2
    } else if (line.contains(thiHoiSumup)) {
      3
    } else if (line.contains(thiDinhSumup)) {
      4
    } else 5

    if (uids.length < 3) {
      AfterSumup(time, uids(0), uids(1), tpe = tpe)
    } else if (uids.length == 3) {
      AfterSumup(time, uids(0), uids(1), uids(2), tpe = tpe)
    } else {
      AfterSumup(time, uids(0), uids(1), uids(2), uids(3), tpe)
    }
  }

  def parseDate(d: String) = {
    DateTime.parse(d, DateTimeFormat.forPattern("dd MMM yyyy HH:mm:ss,SSS")).getMillis/1000
  }

}


case class Line(tpe: Int, startTime: Long, endTime: Long, uid: Int, smfId: Int, smfSession: String, sessionLength: Int) {
  def key = s"$uid-$smfId-$smfSession"
}

case class AfterSumup(time: Int, u1: Int, u2: Int, u3: Int = 0, u4: Int = 0, tpe: Int)

case class DateCfg(start: String, end: String, last: String, server: String)

object DateCfg {
  val format = new SimpleDateFormat("yyyy-MM-dd-HH")
  def init = {
    val current = format.format(new Date())
    DateCfg("2018-08-12-00", current, "2018-08-18-00", "f1")
  }
}