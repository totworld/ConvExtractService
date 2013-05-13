package nsr.domain.client

import java.io.{FileInputStream, BufferedInputStream, File}
import nsr.domain.model.Timeline
import org.slf4j.LoggerFactory
import scala.io.Source
import java.util.zip.GZIPInputStream
import nsr.domain.service.ConvBuilder

object ProcessClient {
  private[this] val logger = LoggerFactory.getLogger("nsr.domain.client.ProecssClient")

  def getStoredUserIDs(targetDirectoryPathStr : String) : Set[Long] = {

    var storedUIDs : collection.mutable.Set[Long] = collection.mutable.Set.empty

    val dirFile = new File(targetDirectoryPathStr + "/stored_timeline")

    if (dirFile == null && !dirFile.isDirectory)
      Set.empty
    else {
      for (i <- 0 until 9) {
        for (j <- 0 until 9) {
          val subDirFile = new File(targetDirectoryPathStr + "/stored_timeline/" + i.toString + "/" + j.toString)
          if (subDirFile != null && subDirFile.isDirectory) {
            subDirFile.list().map(path=>path.split("[/]").last).foreach(storedUID => storedUIDs += storedUID.split("[.]").head.toLong)
          }
        }
      }
      storedUIDs.toSet
    }
  }

  def getTargetJSONPaths(targetDirectoryPathStr : String) : List[String] = {
    val dirFile = new File(targetDirectoryPathStr)

    if (dirFile == null && !dirFile.isDirectory)
      List.empty

    dirFile.listFiles().map(x=>x.getAbsolutePath).filter(x=>x.endsWith(".tweets.gz")).toList
  }

  def unzipTweetFileToSeq(filePath:String) : Seq[String] = {
    Source.fromInputStream(
      new GZIPInputStream(
        new BufferedInputStream(
          new FileInputStream(filePath)))).getLines().toSeq
  }

  def processTimeLineFromLocalFiles(args : Array[String]) {
    if (args.length == 0)
      return

    val targetDirectoryPathStr = args.head

    val processedUIDs = getStoredUserIDs(targetDirectoryPathStr)
    val filePaths = getTargetJSONPaths(targetDirectoryPathStr)

    val convBuilder = new ConvBuilder()

    try {
      filePaths.filter(filePath => !processedUIDs.contains(filePath.split("[/]").last.split("[.]").head.toLong)).foreach(filePath=> {
        val uid : Long = filePath.split("[/]").last.split("[.]").head.toLong
        logger.info("Process Target : " + uid)

        val curTimeLine = new Timeline().getTweets(uid, unzipTweetFileToSeq(filePath))

        convBuilder.buildAndStore(curTimeLine, targetDirectoryPathStr)

        val tempFW = new File(filePath)
        val moveTargetPath = filePath.substring(0, filePath.lastIndexOf("/")) + "/raw_temp/" + filePath.split("[/]").last
        println(filePath + " -> " + moveTargetPath)
        tempFW.renameTo(new File(moveTargetPath))

      })
    } catch {
      case e :
        Exception => {
          logger.error("Error to process local stored JSON timelines")
          logger.error(e.getStackTraceString)
        }
    }
  }
}

object ProcessClientExecuter extends App {
  override def main(args: Array[String]) {
    ProcessClient.processTimeLineFromLocalFiles(args)
  }
}
