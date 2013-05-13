package nsr.domain.model

class Conversation(val tweets:Seq[Tweet]) extends Serializable {

  val sortedTweets = tweets.sortBy(tweet=>tweet.date)
  val id:Long = {
    if (!sortedTweets.isEmpty)
       sortedTweets.head.id
    else
      0L
  }
  val uids = sortedTweets.map(tweet => tweet.uid).toSet
  val headTweetReferID:Long = {
    if (!sortedTweets.isEmpty)
      sortedTweets.head.pid
    else
      0L
  }
  val lastTweetID:Long = {
    if (!sortedTweets.isEmpty)
      sortedTweets.last.id
    else
      0L
  }
  val lastTweetReferID:Long = {
    if (!sortedTweets.isEmpty)
      sortedTweets.last.pid
    else
      0L
  }

  def this() = this(Seq.empty)

  override def toString: String = {
    sortedTweets.map(tweet=>{
      s"${tweet.id}\t${tweet.uid}\t${tweet.pid}\t${tweet.puid}\t${tweet.date}\t${tweet.text.replaceAll("[\t]", " ")}\n"
    }).mkString("")
  }

  def addTweet(tweet : Tweet) : Conversation = {
    new Conversation(tweets :+ tweet)
  }
}
