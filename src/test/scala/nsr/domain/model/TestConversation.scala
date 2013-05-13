package nsr.domain.model

import org.scalatest.FunSuite

class TestConversation extends FunSuite {
  val sampleSortedTweetSeq : Seq[Tweet] = Seq(Tweet(10L, 11L, 12L, 13L, "TEST 1", 14L), Tweet(20L, 21L, 22L, 23L, "TEST 2", 24L))
  val sampleUnSortedTweetSeq : Seq[Tweet] = Seq(Tweet(20L, 21L, 22L, 23L, "TEST 2", 24L), Tweet(10L, 11L, 12L, 13L, "TEST 1", 14L))

  test("To String Test from Conversation") {
    val conversation = new Conversation(sampleSortedTweetSeq)
    assert(conversation.toString() === "10\t11\t12\t13\t14\tTEST 1\n20\t21\t22\t23\t24\tTEST 2\n")
  }

  test("Create Conversation with sorted Tweet Seq") {
    val conversation = new Conversation(sampleSortedTweetSeq)
    assert(conversation.id === 10L)
    assert(conversation.headTweetReferID === 12L)
    assert(conversation.lastTweetID === 20L)
  }

  test("Create Conversation with unsorted Tweet Seq") {
    val conversation = new Conversation(sampleUnSortedTweetSeq)
    assert(conversation.id === 10L)
    assert(conversation.headTweetReferID === 12L)
    assert(conversation.lastTweetID === 20L)
  }

  test("Add Head Tweet") {
    var conversation = new Conversation(sampleUnSortedTweetSeq)

    val newConversation = conversation.addTweet(Tweet(0L, 1L, 2L, 3L, "Test 0", 4L))
    conversation = newConversation

    assert(conversation.id === 0L)
    assert(conversation.headTweetReferID === 2L)
    assert(conversation.lastTweetID === 20L)
  }

  test("Extract UIDS Test") {
    val conversation = new Conversation(sampleUnSortedTweetSeq)

    assert(conversation.uids.toSeq.sortBy(uid=>uid) == Seq(11L, 21L))
  }
}
