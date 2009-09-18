/****
 * Copyright 2009 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ****/

package com.twitter.commons.trie

import org.specs._
import net.lag.logging.Logger
import org.specs.matcher.Matcher

object TrieSpec extends Specification {
  private val log = Logger.get

  /**
   * Matcher for Trie.elements() which returns a List[(Array[Byte], Int, Info)]
   */
  case class matchElements(a: List[(Array[Byte], Int, Any)]) extends Matcher[List[(Array[Byte], Int, Any)]]() {
    def apply(v: => List[(Array[Byte], Int, Any)]) =
        (areEqual(v, a), "elements in trie matched ", "Elements in Trie did not match. Expected: %s  Actual: %s".format(hexStr(v), hexStr(a)))

    def areEqual(a: List[(Array[Byte], Int, Any)], b: List[(Array[Byte], Int, Any)]): Boolean = {
        if (b.isEmpty) return a.isEmpty
        val bh = b.head
        val ah = a.head
        (bh._1.mkString(",") == ah._1.mkString(",")) && (bh._2 == ah._2) && (bh._3 == ah._3) && areEqual(a.tail, b.tail)
    }

    def hexStr(a: List[(Array[Byte], Int, Any)]) = myHexStr(a, "")

    private def myHexStr(a: List[(Array[Byte], Int, Any)], sofar: String): String  = {
      if (a.isEmpty) return sofar
      val ah = a.head
      myHexStr(a.tail, sofar + ";" + List(ah._1.mkString(","), ah._2, ah._3.toString).mkString(","))
    }
  }





  "Trie" should {

    "should start empty" in {
      val testWidth = 4
      var testTrie = new Trie(testWidth)
      testTrie.getNodesWithNonNullInfo().isEmpty mustEqual true
      testTrie.maxDepth() mustEqual 0
      testTrie.size() mustEqual 0
    }

    "return the default info if nothing is added to the trie" in {
      val testWidth = 4
      var testTrie = new Trie(testWidth, "default")
      testTrie.search(1, 1) mustEqual (0, testTrie.defaultInfo())
      testTrie.size() mustEqual 1
      testTrie.elements() must matchElements (List((Array(), 0, "default")))
      testTrie.elementsTrieNodesOnlyList() must matchElements (List((Array(), 0, "default")))
      testTrie.clear()
      testTrie.size() mustEqual 0
    }

    "adding an info on root node should be returned on search" in {
      val testWidth = 4
      var testTrie = new Trie(testWidth, "hello")
      log.trace("should only be one level")
      log.trace(testTrie.toString)
      testTrie.search(1, 1) mustEqual (0, "hello")
      testTrie.maxDepth() mustEqual 0
      testTrie.getNodesWithNonNullInfo() mustEqual List(("",0, "hello"))
      testTrie.exists(0, 0) mustEqual Some("hello")
      testTrie.size() mustEqual 1
    }

    "inserting a node can then be found" in {
      val testWidth = 4
      var testTrie = new Trie(testWidth, "hello")
      val key = 0xa1b2c3d4
      val greeting = "hello2"
      testTrie.insert(key, 2*testWidth, greeting) mustEqual None
      log.trace("after adding 0x%x at level %d: testTrie should be 3 levels", key, 3)
      log.trace(testTrie.toString)
      testTrie.numNodes() mustEqual 3
      testTrie.size() mustEqual 2
      testTrie.elements() must matchElements (List(
        (Array(), 0, "hello"),
        (Array(0xa1.toByte), 2*testWidth, greeting)
      ))
      testTrie.elementsTrieNodesOnlyList() must matchElements (List(
        (Array(0xa1.toByte), 2*testWidth, greeting),
        (Array(), 0, "hello")
      ))

      testTrie.search(key, 4) mustEqual (0, testTrie.defaultInfo())
      testTrie.search(key, 8) mustEqual (8, greeting)
      testTrie.search(key & 0xf0000000, 8) mustEqual (0, testTrie.defaultInfo())
      testTrie.maxDepth() mustEqual 8
      testTrie.getNodesWithNonNullInfo() mustEqual List(("|10|1",8, greeting), ("", 0, "hello"))
      testTrie.exists(key, 2*testWidth) mustEqual Some(greeting)
    }

    "insert two identical values" in {
      val testWidth = 4
      var testTrie = new Trie(testWidth, "hello")
      val key = 0xa2b2c3d4
      testTrie.insert(key, 12, "hello") mustEqual None

      testTrie.exists(key, 12) mustEqual Some("hello")
      testTrie.exists(key, 11) mustEqual None

      log.trace("after first insert: " + testTrie)
      testTrie.search(key, 12) mustEqual (12, "hello")
      testTrie.search(key, 14) mustEqual (12, "hello")
      testTrie.search(key, 24) mustEqual (12, "hello")

      testTrie.insert(key, 12, "hello-again") mustEqual Some("hello")
      log.trace("attempting to overwrite key: 0x%x at depth: 12 with info: %s",key, "hello-again")
      log.trace("after second insert: " + testTrie)
      testTrie.search(key, 12) mustEqual (12, "hello-again")
      testTrie.search(key, 14) mustEqual (12, "hello-again")
      testTrie.search(key, 24) mustEqual (12, "hello-again")
      testTrie.size() mustEqual 2
      testTrie.elements() must matchElements (List(
        (Array(), 0, "hello"),
        (Array(0xa2.toByte, 0xb0.toByte), 12, "hello-again")
      ))

    }

    "longest prefix match on nested prefixes of different width tries" in {
      val key = 0xfa2b3c4d
      val keyLength = 32

      // try for all trie widths of 1 to 32
      for (width <- List(1, 2, 4, 8, 16)) {
        val trie = new Trie(width, 0)
        log.trace("width = " + width)
        var numItems = 1

        for (i <- width to keyLength by width) {
          trie.insert(key, i, i) mustEqual None
          numItems += 1
          trie.insert(key, i, i) mustEqual Some(i)
          if (i > 1) {
            //perturb to have more than 1 children of a node
            trie.insert(0, i, "%d:dummy".format(i)) mustEqual None
            numItems += 1
          }
          trie.exists(key, i) mustEqual Some(i)
          trie.exists(key ^ (1 << 31), i) mustEqual None

          log.trace("trie after inserting length = %d :", i)
          log.trace(trie.toString)
          trie.maxDepth() mustEqual i
          // what was inserted must be returned
          trie.search(key, i) mustEqual (i, i)
          // search of this perturbed key should catch the key of length (i - width) previous value
          trie.search(key, i - 1) mustEqual (i - width, i - width)
          var childrenAlongPath = ""
          for (n <- width to i by width) {
            childrenAlongPath += "|" + ( (key >>> (32 - n)) & ((1<<width)-1) )
          }
          trie.getNodesWithNonNullInfo() mustContain (childrenAlongPath, i, i)
          trie.size() mustEqual numItems
        }
        log.trace("After inserting all keylengths in trie of width = %d : %s", width, trie)
        log.trace("All the trie's keys:" + trie.getNodesWithNonNullInfo())

        // perturb the key and expect the previous length to be returned
        for (i <- width to keyLength by width) {
          trie.search(key ^ (1 << (32 - i)) , 32) mustEqual (i - width, i - width)
        }
      }
    }

    "many prefixes of each even length up to 11 in a binary trie (stress)" in {
      val maxDepth = 11
      val trie = new Trie(1)

      // insert twice (2nd time insert() should return true)
      var numPrefixes = 0
      for (itn <- 1 to 2) {
        numPrefixes = 0
        for (len <- 1 to maxDepth by 2) {
          for (prfx <- 0 until (1 << len)) {
            val p = prfx << (32 - len)
            val info = "0x%x/%d".format(p, len)
            trie.insert(p, len, info) mustEqual (if (itn == 1) None else Some(info))
            numPrefixes += 1
          }
          log.trace("[len = %d] INSERTED %d prefixes", len, numPrefixes)
        }
      }
      trie.getNodesWithNonNullInfo().size mustEqual numPrefixes
      trie.maxDepth() mustEqual maxDepth

      // search
      for (len <- 2 to maxDepth) {
        for (prfx <- 0 until (1 << len)) {
          val p = prfx << (32 - len)
          val (depth, info) = trie.search(p, len)
          if (len % 2 == 1) {
            depth mustEqual len
            info mustEqual "0x%x/%d".format(p, len)
          } else {
            depth mustEqual (len - 1)
            info mustEqual "0x%x/%d".format( p & (~( (1 << (33 - len)) - 1)), len - 1)
          }
        }
        log.trace("[len = %d] Search finished ", len)
      }
    }

    "inserting prefixes at arbitrary levels can then be searched" in {
      val testWidth = 4
      var testTrie = new Trie(testWidth)
      var numItems = 0

      // insert something of length 0 should change root's info
      testTrie.insert(0xabcdef, 0, "hello") mustEqual None
      testTrie.defaultInfo() mustEqual "hello"
      numItems += 1

      val key = 0xa1b2c3d4
      val greeting = "deeper_hello"
      val keyLen = 6
      testTrie.insert(key, keyLen, greeting) mustEqual None
      numItems += 1
      testTrie.size() mustEqual numItems
      testTrie.numNodes() mustEqual 6 // 1 for root, 1 at level 1, 4 at level 2
      log.trace("after adding 0x%x of length %d: testTrie is:", key, keyLen)
      log.trace(testTrie.toString)

      testTrie.search(key, 4) mustEqual (0, testTrie.defaultInfo())
      testTrie.search(key, 7) mustEqual (keyLen, greeting)
      testTrie.search(key, 8) mustEqual (keyLen, greeting)
      testTrie.search(0xafb2c3d4, 8) mustEqual (0, testTrie.defaultInfo())
      testTrie.maxDepth() mustEqual 8
      testTrie.exists(key, keyLen) mustEqual Some(greeting)
      testTrie.getNodesWithNonNullInfo() mustEqual List(
        ("|10|3",keyLen, greeting),
        ("|10|2",keyLen, greeting),
        ("|10|1",keyLen, greeting),
        ("|10|0",keyLen, greeting),
        ("", 0, "hello"))
    }

    "simple deletion test" in {
      val testWidth = 4
      var testTrie = new Trie(testWidth, "hello")
      var numItems = 1
      val key = 0x0
      val greeting = "hello2"
      testTrie.insert(key, 32, greeting) mustEqual None
      numItems += 1
      testTrie.size() mustEqual numItems
      testTrie.maxDepth() mustEqual 32
      testTrie.search(key, 32) mustEqual (32, greeting)

      testTrie.delete(key, 32) mustEqual Some(greeting)
      numItems -= 1
      testTrie.size() mustEqual numItems
      testTrie.exists(key, 32) mustEqual None
      testTrie.delete(key, 32) mustEqual None
    }


    "after deletion, the prefix should not be returned in search" in {
      val testWidth = 4
      var testTrie = new Trie(testWidth, "hello")
      var numItems = 1
      val key = 0xa1b2c3d4
      val greeting = "hello2"
      testTrie.insert(key, 8, greeting) mustEqual None
      numItems += 1
      testTrie.size() mustEqual numItems

      testTrie.search(key, 4) mustEqual (0, testTrie.defaultInfo())
      testTrie.search(key, 8) mustEqual (8, greeting)
      testTrie.maxDepth() mustEqual 8

      testTrie.delete(key, 6) mustEqual None
      testTrie.delete(key, 4) mustEqual None
      testTrie.size() mustEqual numItems
      testTrie.delete(key, 8) mustEqual Some(greeting)
      numItems -= 1
      testTrie.size() mustEqual numItems
      log.trace("after deleting 0x%x of length %d: testTrie is:", key, 8)
      log.trace(testTrie.toString)
      testTrie.exists(key, 8) mustEqual None
      testTrie.search(key, 4) mustEqual (0, testTrie.defaultInfo())
      testTrie.search(key, 8) mustEqual (0, testTrie.defaultInfo())

      testTrie.maxDepth() mustEqual 0
      testTrie.getNodesWithNonNullInfo() mustEqual List(("", 0, "hello"))
    }

    "deletion of nested prefixes should work" in {
      val testWidth = 4
      var testTrie = new Trie(testWidth, 0)
      var numItems = 1
      val key = 0xa1b2c3d4
      for (iteration <- 1 to 2) {
        testTrie.insert(key, 8, 8) mustEqual None
        testTrie.insert(key, 6, 6) mustEqual None
        testTrie.insert(key, 3, 3) mustEqual None
        numItems += 3
        testTrie.size() mustEqual numItems

        log.trace("after doing insertions of length 3, 6, 8")
        log.trace(testTrie.toString)
        testTrie.getNodesWithNonNullInfo() mustEqual List(
          ("|11", 3, 3),
          ("|10|3", 6, 6),
          ("|10|2", 6, 6),
          ("|10|1", 8, 8),
          ("|10|0", 6, 6),
          ("|10", 3, 3),
          ("", 0, 0))
        testTrie.numNodes() mustEqual 7

        for (keyLen <- 0 to 16) {
          val plen = if (keyLen < 3) 0 else if (keyLen < 6) 3 else if (keyLen < 8) 6 else 8
          testTrie.search(key, keyLen) mustEqual (plen, plen)
        }

        testTrie.delete(key, 5) mustEqual None
        testTrie.delete(key, 6) mustEqual Some(6)
        testTrie.delete(key, 6) mustEqual None
        numItems -= 1
        testTrie.size() mustEqual numItems
        log.trace("after deleting 0x%x of length %d: testTrie is:", key, 6)
        log.trace(testTrie.toString)
        testTrie.exists(key, 6) mustEqual None
        testTrie.exists(key, 3) mustEqual Some(3)
        testTrie.exists(key, 8) mustEqual Some(8)
        testTrie.getNodesWithNonNullInfo() mustEqual List(
          ("|11", 3, 3),
          ("|10|1", 8, 8),
          ("|10", 3, 3),
          ("", 0, 0))
        testTrie.maxDepth() mustEqual 8
        testTrie.numNodes() mustEqual 4
        for (keyLen <- 0 to 16) {
          val plen = if (keyLen < 3) 0 else if (keyLen < 8) 3 else 8
          testTrie.search(key, keyLen) mustEqual (plen, plen)
        }

        testTrie.delete(key, 8) mustEqual Some(8)
        testTrie.delete(key, 3) mustEqual Some(3)
        numItems -= 2
        testTrie.size() mustEqual numItems
        testTrie.maxDepth() mustEqual 0
        testTrie.numNodes() mustEqual 1
        log.trace("after deleting all prefixes except that at root")
        log.trace(testTrie.toString)
        testTrie.exists(key, 6) mustEqual None
        testTrie.exists(key, 3) mustEqual None
        testTrie.exists(key, 8) mustEqual None
        testTrie.getNodesWithNonNullInfo() mustEqual List(("", 0, 0))
        for (keyLen <- 0 to 16) {
          testTrie.search(key, keyLen) mustEqual (0, 0)
        }
      }
    }
  }
}
