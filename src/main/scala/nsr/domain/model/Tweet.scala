package nsr.domain.model

case class Tweet(id:Long = 0L, uid:Long = 0L, pid:Long = 0L, puid:Long = 0L, text:String = "", date:Long = 0L) extends Serializable
