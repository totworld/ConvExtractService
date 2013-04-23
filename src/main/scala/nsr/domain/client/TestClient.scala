package nsr.domain.client

import scala.io.Source
import java.io._
import scala.collection.mutable
import java.util.zip.GZIPInputStream
import scala.Serializable


object TestClient extends App {

  case class Tweet(id:Long, uid:Long, pid:Long, puid:Long, text:String, date:Long) extends Serializable

  def parseJson(jsonStr : Seq[String]) : Seq[Map[String, Any]] = {

    val tweets = jsonStr.map(tweet => {
      scala.util.parsing.json.JSON.parseFull(tweet)
    })

    tweets.map(x=>x.get.asInstanceOf[Map[String, Any]])
  }

  def getTweets(uid:Long, timeLine:Seq[Map[String, Any]]) : Map[Long, Seq[Tweet]] = {

    val dateFormat = new java.text.SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy", new java.util.Locale("US", "US"))
    dateFormat.format(new java.util.Date())

    val tweets = timeLine.map(x=>{
      new Tweet(
        x.getOrElse("id", 0).asInstanceOf[Double].asInstanceOf[Long],
        uid,
        x.getOrElse("in_reply_to_status_id", 0).asInstanceOf[Double].asInstanceOf[Long],
        x.getOrElse("in_reply_to_user_id", 0).asInstanceOf[Double].asInstanceOf[Long],
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

    val resultFW = new java.io.FileWriter(targetDirectoryPathStr + "/conv/" + uid + "_" + puid + ".log")

    val menBetweenAB = (curTweet.getOrElse(puid, List.empty) ++ opponentTweet.getOrElse(uid, List.empty)).sortBy(x=>x.date)

    println("--Number of Mention : " + menBetweenAB.size)

    if (menBetweenAB.size > 1) {
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
    }

    resultFW.close()
  }

  def unzipTweetFileToSeq(filePath:String) : Seq[String] = {
    Source.fromInputStream(
      new GZIPInputStream(
        new BufferedInputStream(
          new FileInputStream(filePath)))).getLines.toSeq
  }

  def storeToPersistenceLayer(targetTweets:collection.mutable.Map[Long, Map[Long, Seq[Tweet]]], targetDirectoryPathStr : String) {
    targetTweets.foreach(curTweets => {
      val curUID = curTweets._1
      curTweets._2.foreach(curOPTweet => {
        val curPUID = curOPTweet._1

        val targetDirFW = new File(targetDirectoryPathStr + "/stored/" + curUID + "/")
        if (!targetDirFW.exists())
          targetDirFW.mkdir()

        val storeTargetFW = new ObjectOutputStream(new FileOutputStream(targetDirectoryPathStr + "/stored/" + curUID + "/" + curUID + "_" + curPUID + ".obj", true))
        storeTargetFW.writeObject(curOPTweet._2)
        storeTargetFW.close
      })
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
      if (!tweets.contains(uid)) {

        val timeLine:Seq[Map[String, Any]] = parseJson(unzipTweetFileToSeq(filePath))
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

            if (new File(targetDirectoryPathStr + "/stored/" + puid + "/" + puid + "_0.log").exists()) {
              val inputFW = new ObjectInputStream(new FileInputStream(targetDirectoryPathStr + "/stored/" + puid + "/" + puid + "_" + uid + ".obj"))
              opponentTweet += 0L -> inputFW.readObject.asInstanceOf[Seq[Tweet]]
            }

            if (new File(targetDirectoryPathStr + "/stored/" + puid + "/" + puid + "_" + uid + ".log").exists()) {
              val inputFW = new ObjectInputStream(new FileInputStream(targetDirectoryPathStr + "/stored/" + puid + "/" + puid + "_" + uid + ".obj"))
              opponentTweet += uid -> inputFW.readObject.asInstanceOf[Seq[Tweet]]
            }

            println("-Opponent Target : " + puid)
            extractConversation(targetDirectoryPathStr, uid, curTweet, puid, opponentTweet)
          }
        })

        if (tweets.keys.size > 100) {
          storeToPersistenceLayer(tweets.take(10).asInstanceOf[collection.mutable.Map[Long, Map[Long, Seq[Tweet]]]], targetDirectoryPathStr)
          tweets.take(10).foreach(x=>tweets.remove(x._1))
        }

        tweets += uid -> curTweet.filter(x => !processedOpponents.contains(x._1))
      }

      processedUIDs += uid
      fw.append(uid + "\n")
      fw.flush

    })
    storeToPersistenceLayer(tweets.asInstanceOf[collection.mutable.Map[Long, Map[Long, Seq[Tweet]]]], targetDirectoryPathStr)

    fw.close()
  }
}
