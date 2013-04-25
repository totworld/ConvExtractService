package nsr.domain.client

import scala.io.Source
import java.io._
import scala.collection.mutable
import java.util.zip.GZIPInputStream
import scala.Serializable
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.core.`type`.TypeReference

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

object TestClient extends App {

  case class Tweet(id:Long, uid:Long, pid:Long, puid:Long, text:String, date:Long) extends Serializable

  def parseJson(jsonStr : Seq[String]) : Seq[Map[String, Any]] = {

    val tweets = jsonStr.map(tweet => {
      scala.util.parsing.json.JSON.parseFull(tweet)
    })

    tweets.map(x=>x.get.asInstanceOf[Map[String, Any]])
  }

  def parseJsonByJackson(jsonStr : Seq[String]) : Seq[Map[String, Any]] = {

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val jsonObject = jsonStr.map(tweet => {
      mapper.readValue[Map[String, Any]](tweet, new TypeReference[Map[String, Any]] {})
    }).toSeq

    jsonObject
  }

  def getTweets(uid:Long, timeLine:Seq[Map[String, Any]]) : Map[Long, Seq[Tweet]] = {

    val dateFormat = new java.text.SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy", new java.util.Locale("US", "US"))
    dateFormat.format(new java.util.Date())

    val tweets = timeLine.map(x=>{
      new Tweet(
        x.getOrNElse("id", 0L),
        uid,
        x.getOrNElse("in_reply_to_status_id", 0L),
        x.getOrNElse("in_reply_to_user_id", 0L),
        x.get("text").mkString.replaceAll("[\t\n\r]", " "),
        dateFormat.parse(x.get("created_at").mkString).getTime
      )
    })

    val tweetsByPUID:Map[Long, Seq[Tweet]] = tweets.groupBy(x => x.puid)

    tweetsByPUID
  }

  def getTargetPaths(dirPath : String) : List[String] = {
    val dirFile = new File(dirPath)

    if (dirFile == null && !dirFile.isDirectory)
      List.empty

    dirFile.listFiles().map(x=>x.getAbsolutePath).filter(x=>x.endsWith(".tweets.gz")).toList
  }

  def extractConversation(targetDirectoryPathStr : String, uid : Long, curTweet : Map[Long, Seq[Tweet]], puid : Long, opponentTweet : Map[Long, Seq[Tweet]]) {

    val menBetweenAB = (curTweet.getOrElse(puid, List.empty) ++ opponentTweet.getOrElse(uid, List.empty)).sortBy(x=>x.date)

    println("--Number of Mention : " + menBetweenAB.size)

    if (menBetweenAB.size > 1) {

      var targetConvFilePath = targetDirectoryPathStr + "/conv/" + uid + "_" + puid + ".log"
      if (uid > puid) {
        targetConvFilePath = targetDirectoryPathStr + "/conv/" + puid + "_" + uid + ".log"
      }

      val resultFW = new java.io.FileWriter(targetConvFilePath, true)

      var lastID : Long = 0L
      menBetweenAB.foreach(tweet => {
        if (lastID != tweet.pid) {
          resultFW.append("\n")
          if (tweet.puid == uid) {
            val candidateTweet = curTweet.getOrElse(0, List.empty).filter(x=>x.id == tweet.pid)

            if (!candidateTweet.isEmpty) {
              val headTweet = candidateTweet.head
              resultFW.append(headTweet.uid + "\t" + headTweet.date + "\t" + headTweet.text + "\n")
            } else {
              resultFW.append(tweet.puid + "\t" + tweet.pid + "\t" + "Missing Tweet\n")
              println("Missing Head Tweet : " + tweet.pid)
            }
          } else if (tweet.puid == puid) {
            val candidateTweet = opponentTweet.getOrElse(0, List.empty).filter(x=>x.id == tweet.pid)

            if (!candidateTweet.isEmpty) {
              val headTweet = candidateTweet.head
              resultFW.append(headTweet.uid + "\t" + headTweet.date + "\t" + headTweet.text + "\n")
            } else {
              resultFW.append(tweet.puid + "\t" + tweet.pid + "\t" + "Missing Tweet\n")
              println("Missing Head Tweet : " + tweet.pid)
            }
          } else
            resultFW.append(tweet.puid + "\t" + tweet.pid + "\t" + "Missing Tweet\n")
        }
        resultFW.append(tweet.uid + "\t" + tweet.date + "\t" + tweet.text + "\n")

        println(tweet.text)

        lastID = tweet.id
      })
      resultFW.close()
    }

  }

  def unzipTweetFileToSeq(filePath:String) : Seq[String] = {
    Source.fromInputStream(
      new GZIPInputStream(
        new BufferedInputStream(
          new FileInputStream(filePath)))).getLines().toSeq
  }

  def storeToPersistenceLayer(targetTweets:collection.mutable.Map[Long, Map[Long, Seq[Tweet]]], targetDirectoryPathStr : String) {
    targetTweets.foreach(curTweets => {
      val curUID = curTweets._1

      val idx = curUID % 10
      val idx2 = curUID / 10 % 10

      val targetDirFW = new File(targetDirectoryPathStr + "/stored/" + idx + "/" + idx2 + "/" + curUID)

      var isTargetDirExist = targetDirFW.exists()
      if (!isTargetDirExist)
        isTargetDirExist = targetDirFW.mkdir()

      if (isTargetDirExist) {
        curTweets._2.foreach(curOPTweet => {
          val curPUID = curOPTweet._1
          val storeTargetFW = new ObjectOutputStream(new FileOutputStream(targetDirectoryPathStr + "/stored/" + idx + "/" + idx2 + "/" + curUID + "/" + curUID + "_" + curPUID + ".obj", true))
          storeTargetFW.writeObject(curOPTweet._2)
          storeTargetFW.close()
        })
      } else
        println("Failed to make directory for " + curUID)
    })
  }

  override def main(args: Array[String]) {

    val targetDirectoryPathStr = args.head

    var processedUIDs : collection.mutable.Set[Long] = collection.mutable.Set.empty

    if (new File(targetDirectoryPathStr + "/processed.log").exists()) {
      val fr = Source.fromFile(targetDirectoryPathStr + "/processed.log")

      fr.getLines().foreach(x => processedUIDs += x.toLong)
      fr.close()
    }

    val filePaths = getTargetPaths(targetDirectoryPathStr)

    val fw = new java.io.FileWriter(targetDirectoryPathStr + "/processed.log", true)

    var tweets : collection.mutable.Map[Long, Map[Long, Seq[Tweet]]] = collection.mutable.Map.empty

    filePaths.filter(filePath => !processedUIDs.contains(filePath.split("[/]").last.split("[.]").head.toLong)).foreach(filePath=> {
      val uid : Long = filePath.split("[/]").last.split("[.]").head.toLong

      println("Process Target : " + uid)
      if (!processedUIDs.contains(uid)) {

        val timeLine:Seq[Map[String, Any]] = parseJsonByJackson(unzipTweetFileToSeq(filePath))
        val curTweet = getTweets(uid, timeLine)

        var processedOpponents : mutable.HashSet[Long] = collection.mutable.HashSet.empty

        curTweet.filter(opponent => opponent._1 != 0L).foreach(opponent => {
          val puid : Long = opponent._1

          if (tweets.contains(puid)) {
            val opponentTweet : Map[Long, Seq[Tweet]] = tweets.getOrElse(puid, Map.empty)

            println("-Opponent Target : " + puid)
            extractConversation(targetDirectoryPathStr, uid, curTweet, puid, opponentTweet)

            processedOpponents += puid
          } else if (processedUIDs.contains(puid)) {
            var opponentTweet: Map[Long, Seq[Tweet]] = Map.empty

            val idx = puid % 10
            val idx2 = puid / 10 % 10

            if (new File(targetDirectoryPathStr + "/stored/" + idx + "/" + idx2 + "/" + puid + "/" + puid + "_0.obj").exists()) {
              val inputFW = new ObjectInputStream(new FileInputStream(targetDirectoryPathStr + "/stored/" + idx + "/" + idx2 + "/" + puid + "/" + puid + "_0.obj"))
              opponentTweet += 0L -> inputFW.readObject.asInstanceOf[Seq[Tweet]]
              println("<< from Disk : " + puid + " - 0")
            }

            if (new File(targetDirectoryPathStr + "/stored/" + idx + "/" + idx2 + "/ " + puid + "/" + puid + "_" + uid + ".obj").exists()) {
              val inputFW = new ObjectInputStream(new FileInputStream(targetDirectoryPathStr + "/stored/" + idx + "/" + idx2 + "/" + puid + "/" + puid + "_" + uid + ".obj"))
              opponentTweet += uid -> inputFW.readObject.asInstanceOf[Seq[Tweet]]
              println("<< from Disk : " + puid + " - " + uid)
            }

            println("-Opponent Target : " + puid)
            extractConversation(targetDirectoryPathStr, uid, curTweet, puid, opponentTweet)
          }
        })

        if (tweets.keys.size > 100) {
          storeToPersistenceLayer(tweets.take(10), targetDirectoryPathStr)
          tweets.take(10).foreach(x=>{
            println(">> Stored to disk : " + x._1)
            tweets.remove(x._1)
          })
        }

        tweets += uid -> curTweet.filter(x => !processedOpponents.contains(x._1))

        new File(filePath).delete()

        processedUIDs += uid
        fw.append(uid + "\n")
        fw.flush()
      }

    })
    storeToPersistenceLayer(tweets, targetDirectoryPathStr)

    fw.close()
  }
}
