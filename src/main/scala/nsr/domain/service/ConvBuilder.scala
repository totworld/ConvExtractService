package nsr.domain.service

import nsr.domain.model.{Tweet, Timeline, Conversation}
import scala.collection.mutable
import java.io._
import nsr.domain.model.Tweet
import org.slf4j.LoggerFactory

class ConvBuilder() {
  private[this] val logger = LoggerFactory.getLogger("nsr.domain.service.ConvBuilder")

  val cachedTimelineLimit = 500
  val cachedConvLimit = 500

  var cachedTimelines : collection.mutable.Map[Long, Timeline] = mutable.Map.empty
  var cachedConvs : collection.mutable.Seq[Conversation] = mutable.Seq.empty
  var storedTimelineUIDs : collection.mutable.Set[Long] = mutable.Set.empty
  var storedConvIDs : collection.mutable.Map[Long, Long] = mutable.Map.empty

  def storeConversation(targetDirectoryPathStr : String, conversation : Conversation) : Boolean = {
    var isSuccess = true

    val idx = conversation.id % 10
    val idx2 = conversation.id / 10 % 10

    val targetDirFW = new File(targetDirectoryPathStr + "/conv/" + idx + "/" + idx2)
    val isTargetDirExist = targetDirFW.exists()
    if (isTargetDirExist || targetDirFW.mkdir()) {
      val storeTargetFW = new FileOutputStream(targetDirectoryPathStr + "/conv/" + idx + "/" + idx2 + "/" + conversation.id + ".conv", true)
      storeTargetFW.write(conversation.toString().getBytes)
      storeTargetFW.close()

      val storeTargetObjFW = new ObjectOutputStream(new FileOutputStream(targetDirectoryPathStr + "/conv/" + idx + "/" + idx2 + "/" + conversation.id + ".obj", true))
      storeTargetObjFW.writeObject(conversation)
      storeTargetObjFW.close()
    } else
      isSuccess = false

    isSuccess
  }

  def storeTimeline(targetDirectoryPathStr : String, timeline : Timeline) : Boolean = {

      var isSuccess = true

      val uid = timeline.uid

      val idx = uid % 10
      val idx2 = uid / 10 % 10

      val targetDirFW = new File(targetDirectoryPathStr + "/stored_timeline/" + idx + "/" + idx2)

      val isTargetDirExist = targetDirFW.exists()
      if (isTargetDirExist || targetDirFW.mkdir) {
        val storeTargetFW = new ObjectOutputStream(new FileOutputStream(targetDirectoryPathStr + "/stored_timeline/" + idx + "/" + idx2 + "/" + uid + ".obj", true))
        storeTargetFW.writeObject(timeline)
        storeTargetFW.close()
      } else {
        logger.error("Failed to make directory for " + uid)
        isSuccess = false
      }

    isSuccess
  }

  def getStoredTimeline(targetDirectoryPathStr : String, targetUID : Long) : Timeline = {
    val idx = targetUID % 10
    val idx2 = targetUID / 10 % 10

    var storedTimeline = new Timeline
    if (new File(targetDirectoryPathStr + ("/stored_timeline/" + idx.toString + "/" + idx2.toString + "/" + targetUID.toString + ".obj").replaceAll("[ ]", "")).exists()) {
      val inputFW = new ObjectInputStream(new FileInputStream(targetDirectoryPathStr + ("/stored_timeline/" + idx.toString + "/" + idx2.toString + "/" + targetUID.toString + ".obj").replaceAll("[ ]", "")))

      storedTimeline = inputFW.readObject.asInstanceOf[Timeline]
    }

    storedTimeline
  }

  def buildAndStore(timeline : Timeline,
            targetDirectoryPathStr : String) = {

    val processedTweetIDs: collection.mutable.Set[Long] = collection.mutable.Set.empty
    var curConversationTweets : collection.mutable.Set[Tweet] = collection.mutable.Set.empty

    logger.debug("> " + timeline.uid.toString + "(" + timeline.tweets.size + ")")

    timeline.tweets.foreach(tweet=>{

      if (!processedTweetIDs.contains(tweet.id)) {
        if (tweet.pid != 0L) {

          curConversationTweets.clear()

          var curTweet = tweet

          var cont = true
          while (cont && !processedTweetIDs.contains(curTweet.id)) {
            logger.debug(">> " + curTweet.id.toString + " from " + curTweet.uid.toString)

            if (curConversationTweets.filter(tweet=>tweet.id == curTweet.id).size == 0)
              curConversationTweets.add(curTweet)

            if (curTweet.uid == timeline.uid) {
              processedTweetIDs.add(curTweet.id)
            }

            val nextTargetID = curTweet.pid
            val nextTargetUID = curTweet.puid

            if (nextTargetID == 328472338565828610L)
              println("Target")

            if (nextTargetID == 0L)
              cont = false
            else {
              if (nextTargetUID == timeline.uid) {
                val matchedTweet = timeline.tweets.filter(tweet => tweet.id == nextTargetID)
                if (matchedTweet.size > 0) {
                  logger.debug(">>> Found in current Timeline(" + matchedTweet.head.id + ")")
                  curTweet = matchedTweet.head
                } else
                  cont = false
              } else if (cachedTimelines.contains(nextTargetUID)) {
                val matchedTweet = cachedTimelines.get(nextTargetUID).head.tweets.filter(tweet => tweet.id == nextTargetID)
                if (matchedTweet.size > 0) {
                  logger.debug(">>> Found in cachced Timeline(" + matchedTweet.head.id + ")")
                  curTweet = matchedTweet.head
                } else
                  cont = false
              } else if (storedTimelineUIDs.contains(nextTargetUID)) {
                val storedTimeline = getStoredTimeline(targetDirectoryPathStr, nextTargetUID)
                if (cachedTimelines.size > cachedTimelineLimit)
                  cachedTimelines = cachedTimelines.takeRight(cachedTimelineLimit / 2)
                cachedTimelines += storedTimeline.uid -> storedTimeline

                val matchedTweet = storedTimeline.tweets.filter(tweet => tweet.id == nextTargetID)
                if (matchedTweet.size > 0) {
                  logger.debug(">>> Found in stored Timeline(" + matchedTweet.head.id + ")")
                  curTweet = matchedTweet.head
                } else
                  cont = false
              } else if (storedConvIDs.contains(nextTargetID)) {
                val mergeTargetConvID : Long = storedConvIDs.get(nextTargetID).get
                val idx = mergeTargetConvID % 10
                val idx2 = mergeTargetConvID / 10 % 10

                if (cachedConvs.filter(convTweet=>convTweet.id == mergeTargetConvID).size > 0) {
                  logger.debug(">>> Found in cached Conversation(" + curTweet.id + ")")
                  val targetCachedConv = cachedConvs.filter(convTweet=>convTweet.id == mergeTargetConvID)
                  targetCachedConv.head.tweets.foreach(cachedTweet => {
                    if (curConversationTweets.filter(tweet=>tweet.id == cachedTweet.id).size == 0)
                      curConversationTweets.add(cachedTweet)
                  })
                  cachedConvs = cachedConvs.filter(cachedConv=>cachedConv.id != mergeTargetConvID)
                } else {
                  logger.debug(">>> Found in stored Conversation(" + curTweet.id + ")")
                  val storedCachedConvObjInputFW = new ObjectInputStream(new FileInputStream(targetDirectoryPathStr + "/conv/" + idx + "/" + idx2 + "/" + mergeTargetConvID + ".obj"))
                  val curStoredConv : Conversation = storedCachedConvObjInputFW.readObject().asInstanceOf[Conversation]
                  curStoredConv.tweets.foreach(cachedTweet => {
                    if (curConversationTweets.filter(tweet=>tweet.id == cachedTweet.id).size == 0)
                      curConversationTweets.add(cachedTweet)
                  })
                }
                removeTargetStoredConv(mergeTargetConvID, targetDirectoryPathStr)
                if (mergeTargetConvID != curTweet.id)
                  removeTargetStoredConv(curTweet.id, targetDirectoryPathStr)

                storedConvIDs = storedConvIDs.filter(storedConvID => storedConvID._2 != mergeTargetConvID)

                curTweet = curConversationTweets.toSeq.sortBy(curCachedConvTweet => curCachedConvTweet.date).head


                cont = false
              } else
                cont = false
            }
          }

          var conversation = new Conversation(curConversationTweets.toSeq)

          if (storedConvIDs.contains(conversation.id)) {
            val mergeTargetConvID = storedConvIDs.get(conversation.id).get
            val idx = mergeTargetConvID % 10
            val idx2 = mergeTargetConvID / 10 % 10

            logger.debug("> Merge Conv : " + mergeTargetConvID.toString + " + " + conversation.id.toString)

            if (cachedConvs.filter(convTweet=>convTweet.id == mergeTargetConvID).size > 0) {
              val targetCachedConv = cachedConvs.filter(convTweet=>convTweet.id == mergeTargetConvID)
              targetCachedConv.head.tweets.foreach(cachedTweet => {
                if (curConversationTweets.filter(tweet=>tweet.id == cachedTweet.id).size == 0)
                  curConversationTweets.add(cachedTweet)
              })
              cachedConvs = cachedConvs.filter(cachedConv=>cachedConv.id != mergeTargetConvID)
            } else if ((new File(targetDirectoryPathStr + "/conv/" + idx + "/" + idx2 + "/" + mergeTargetConvID + ".obj")).exists()) {
              val storedCachedConvObjInputFW = new ObjectInputStream(new FileInputStream(targetDirectoryPathStr + "/conv/" + idx + "/" + idx2 + "/" + mergeTargetConvID + ".obj"))
              val curStoredConv : Conversation = storedCachedConvObjInputFW.readObject().asInstanceOf[Conversation]
              curStoredConv.tweets.foreach(cachedTweet => {
                if (curConversationTweets.filter(tweet=>tweet.id == cachedTweet.id).size == 0)
                  curConversationTweets.add(cachedTweet)
              })
            } else {
              logger.error("Failed to merge conversation for " + mergeTargetConvID.toString)
            }

            removeTargetStoredConv(mergeTargetConvID, targetDirectoryPathStr)
            storedConvIDs = storedConvIDs.filter(storedConvID => storedConvID._2 != mergeTargetConvID)

            conversation = new Conversation(curConversationTweets.toSeq)

            if (conversation.id != mergeTargetConvID)
              removeTargetStoredConv(conversation.id, targetDirectoryPathStr)
          }

          if (cachedConvs.size > cachedConvLimit)
            cachedConvs = cachedConvs.takeRight(cachedConvLimit / 2)
          cachedConvs = cachedConvs :+ conversation

          if (!storeConversation(targetDirectoryPathStr, conversation))
            logger.error("Error to store target conversation : " + conversation.id.toString)
          else {
            logger.debug("> Stored Conv(" + conversation.id.toString + " : " + conversation.tweets.size + ")")
            conversation.tweets.foreach(tweet=> {
              storedConvIDs += tweet.id->conversation.id
            })
          }

        }
      }
    })

    processedTweetIDs.clear()

    if (storeTimeline(targetDirectoryPathStr, timeline))
      storedTimelineUIDs += timeline.uid

    if (cachedTimelines.size > cachedTimelineLimit)
      cachedTimelines = cachedTimelines.takeRight(cachedTimelineLimit / 2)
    cachedTimelines += timeline.uid -> timeline
  }


  def removeTargetStoredConv(id: Long, targetDirectoryPathStr: String): AnyVal = {
    val idx = id % 10
    val idx2 = id / 10 % 10

    val storedCachedConvFW = new File(targetDirectoryPathStr + "/conv/" + idx + "/" + idx2 + "/" + id + ".conv")
    if (storedCachedConvFW.exists())
      storedCachedConvFW.delete()
    val storedCachedConvObjFW = new File(targetDirectoryPathStr + "/conv/" + idx + "/" + idx2 + "/" + id + ".obj")
    if (storedCachedConvObjFW.exists())
      storedCachedConvObjFW.delete()
  }
}
