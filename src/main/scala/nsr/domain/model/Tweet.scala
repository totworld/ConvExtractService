package nsr.domain.model

case class Tweet(id:Long, uid:Long, pid:Long, puid:Long, text:String, date:Long) extends Serializable