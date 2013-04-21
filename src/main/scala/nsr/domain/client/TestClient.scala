package nsr.domain.client

import scala.io.Source
import java.io.File
import scala.collection.mutable

object TestClient extends App {

  case class Tweet(id:Long, uid:Long, pid:Long, puid:Long, text:String, date:Long) {}

  def parseJson(jsonStr : String) : List[Map[String, Any]] = {
    val jsonObject = scala.util.parsing.json.JSON.parseFull(jsonStr)
    jsonObject.get.asInstanceOf[List[Map[String, Any]]]
  }

  def getTweets(uid:Long, timeLine:List[Map[String, Any]]) : Map[Long, List[Tweet]] = {

    val dateFormat = new java.text.SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy")
    dateFormat.format(new java.util.Date())

    val tweets = timeLine.map(x=>{
      new Tweet(
        x.getOrElse("id", 0).asInstanceOf[Double].asInstanceOf[Long],
        uid,
        x.getOrElse("in_reply_to_status_id", 0).asInstanceOf[Double].asInstanceOf[Long],
        x.getOrElse("in_reply_to_user_id", 0).asInstanceOf[Double].asInstanceOf[Long],
        x.get("text").mkString,
        dateFormat.parse(x.get("created_at").mkString).getTime
      )
    })

    val tweetsByPUID:Map[Long, List[Tweet]] = tweets.groupBy(x => x.puid)

    tweetsByPUID
  }

  def getTargetPaths(dirPath : String) : List[String] = {
    val dirFile = new File(dirPath)

    if (dirFile == null && !dirFile.isDirectory)
      List.empty

    dirFile.listFiles().map(x=>x.getAbsolutePath).filter(x=>x.endsWith(".json.gz")).toList
  }

  def extractConversation(uid : Long, curTweet : Map[Long, List[Tweet]], puid : Long, opponentTweet : Map[Long, List[Tweet]]) {
    val menBetweenAB = (curTweet.getOrElse(puid, List.empty) ++ opponentTweet.getOrElse(uid, List.empty)).sortBy(x=>x.date)

    var lastID : Long = 0L
    menBetweenAB.foreach(tweet => {
      if (lastID != tweet.pid) {
        println()
        if (tweet.puid == uid) {
          val headTweet = curTweet.getOrElse(0, List.empty).filter(x=>x.id == tweet.pid).head
          println(headTweet.uid + "\t" + headTweet.date + "\t" + headTweet.text)
        } else if (tweet.puid == puid) {
          val headTweet = opponentTweet.getOrElse(0, List.empty).filter(x=>x.id == tweet.pid).head
          println(headTweet.uid + "\t" + headTweet.date + "\t" + headTweet.text)
        } else
          println("Missing Tweet : " + tweet.puid + " - " + tweet.pid)
      }
      println(tweet.uid + "\t" + tweet.date + "\t" + tweet.text)
      lastID = tweet.id
    })
  }

  override def main(args: Array[String]) {
    val targetDirectoryPathStr = "/Users/paulkim/Source Code/ConvExtractService/data"

    val filePaths = getTargetPaths(targetDirectoryPathStr)

    var tweets : collection.mutable.Map[Long, Map[Long, List[Tweet]]] = collection.mutable.Map.empty
    filePaths.foreach(filePath=> {
      val uid : Long = filePath.split("[/]").last.split("[.]").head.toLong

      if (!tweets.contains(uid)) {
        val timeLine:List[Map[String, Any]] = parseJson(Source.fromFile(filePath).getLines().toSeq.mkString)
        val curTweet = getTweets(uid, timeLine)

        var processedOpponents : mutable.HashSet[Long] = collection.mutable.HashSet.empty

        curTweet.filter(opponent => opponent._1 != 0L).foreach(opponent => {
          val puid : Long = opponent._1

          if (tweets.contains(puid)) {
            val opponentTweet : Map[Long, List[Tweet]] = tweets.getOrElse(puid, Map.empty)

            extractConversation(uid, curTweet, puid, opponentTweet)

            processedOpponents += puid
          }
        })
        tweets += uid -> curTweet.filter(x => !processedOpponents.contains(x._1))
      }
    })
  }
}
