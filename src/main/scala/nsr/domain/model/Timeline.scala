package nsr.domain.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.core.`type`.TypeReference
import org.slf4j.LoggerFactory

object ExtendedMap {
  implicit def String2ExtendedString(s: Map[String, Any]) = new MapGetOrElseExtension(s)
}

class MapGetOrElseExtension(s: Map[String, Any]) {
  implicit def String2ExtendedString(s: Map[String, Any]) = new MapGetOrElseExtension(s)
  def getOrNElse[Long](targetKey : String, t : Long) : Long  = {
    if (s == null || s.isEmpty) {
      t
    } else {
      try
        s.getOrElse(targetKey, t).toString.toLong.asInstanceOf[Long]
      catch {
        case e:
          Exception => t
      }
    }
  }
}

import ExtendedMap._

class Timeline(val uid : Long = 0L, val tweets : Seq[Tweet] = Seq.empty) extends Serializable {

  private[this] val logger = LoggerFactory.getLogger("nsr.domain.model.Timeline")

  def parseJsonByJackson(jsonStr : Seq[String]) : Seq[Map[String, Any]] = {

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val jsonObject = jsonStr.map(tweet => {
      mapper.readValue[Map[String, Any]](tweet, new TypeReference[Map[String, Any]] {})
    }).toSeq

    jsonObject
  }

  def isValidJson(target: Map[String, Any]): Boolean = {

    var result = false

    if (target.contains("id")
      && target.contains("text")
      && target.contains("created_at")
      && target.get("created_at").mkString.length > 20)
      result = true
    else
      logger.warn("[getTweets]Mandatory Field is missing.")

    result
  }

  def getTweets(uid:Long, jsonStr : Seq[String]) : Timeline = {

    logger.debug(">JsonParser Target Line Number : " + jsonStr.size)

    val timelineJsonMap = parseJsonByJackson(jsonStr)

    logger.debug(">Json Object Number : " + timelineJsonMap.size)

    val dateFormat = new java.text.SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy", new java.util.Locale("US", "US"))
    dateFormat.format(new java.util.Date())

    val tweets = timelineJsonMap.map(x=>{
      if (isValidJson(x)) {
        new Tweet(
          x.getOrNElse("id", 0L),
          uid,
          x.getOrNElse("in_reply_to_status_id", 0L),
          x.getOrNElse("in_reply_to_user_id", 0L),
          x.get("text").mkString.replaceAll("[\t\n\r]", " "),
          dateFormat.parse(x.get("created_at").mkString).getTime
        )
      } else
        new Tweet(0L, 0L, 0L, 0L, "", 0L)
    }).toSeq.sortBy(tweet=>Long.MaxValue - tweet.date)

    logger.debug(">Result Tweet Number : " + tweets.size)

    new Timeline(uid, tweets)
  }
}
