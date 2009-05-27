/** Copyright 2009 Twitter, Inc. */
package com.twitter.commons.trie

import org.specs._
object TrieSpec extends Specification {

  "Trie" should {

    "should start with a non-null root" in {
      val testDepth = 4
      var testTrie = new Trie(testDepth)
      testTrie.getItems().isEmpty mustEqual true
      testTrie.root.info = "default"
      testTrie mustNotBe null
      testTrie.root mustNotBe null
      testTrie.maxDepth() mustEqual 0
    }

    "empty testTrie should have no items" in {
      val testDepth = 4
      var testTrie = new Trie(testDepth, "default")
      testTrie.search(1, 1) mustEqual (0, testTrie.root.info)
      // println("empty trie should be -- well -- empty")
      // println(testTrie)
    }

    "adding an info on root node should be returned on search" in {
      val testDepth = 4
      var testTrie = new Trie(testDepth, "hello")
      // println("should only be one level")
      // println(testTrie)
      testTrie.search(1, 1) mustEqual (0, "hello")
      testTrie.maxDepth() mustEqual 0
      testTrie.getItems() mustEqual List(("",0, "hello"))
      testTrie.exists(0, 0) mustEqual (true, "hello")
    }

    "inserting a node can then be found" in {
      val testDepth = 4
      var testTrie = new Trie(testDepth, "hello")
      val key = 0xa1b2c3d4
      val greeting = "hello2"
      val isDup = testTrie.insert(key, 2*testDepth, greeting)
      isDup mustEqual false
      // println("after adding 0x%x at level %d: testTrie should be 3 levels".format(key, 3))
      // println(testTrie)

      testTrie.search(key, 4) mustEqual (0, testTrie.root.info)
      testTrie.search(key, 8) mustEqual (8, greeting)
      testTrie.search(key & 0xf0000000, 8) mustEqual (0, testTrie.root.info)
      testTrie.maxDepth() mustEqual 8
      testTrie.getItems() mustEqual List(("|10|1",8, greeting), ("", 0, "hello"))
      testTrie.exists(key, 2*testDepth) mustEqual (true, greeting)
    }

    "insert two identical values" in {
      val testDepth = 4
      var testTrie = new Trie(testDepth, "hello")
      val key = 0xa2b2c3d4
      var isDup = testTrie.insert(key, 12, "hello")
      isDup mustEqual false

      testTrie.exists(key, 12) mustEqual (true, "hello")
      testTrie.exists(key, 11)._1 mustEqual false

      // println("after first insert: " + testTrie)
      testTrie.search(key, 12) mustEqual (12, "hello")
      testTrie.search(key, 14) mustEqual (12, "hello")
      testTrie.search(key, 24) mustEqual (12, "hello")

      isDup = testTrie.insert(key, 12, "hello-again")
      isDup mustEqual true
      // println("attempting to overwrite key: 0x%x at depth: 12 with info: %s".format(key, "hello-again"))
      // println("after second insert: " + testTrie)
      testTrie.search(key, 12) mustEqual (12, "hello-again")
      testTrie.search(key, 14) mustEqual (12, "hello-again")
      testTrie.search(key, 24) mustEqual (12, "hello-again")
    }

    "longest prefix match on nested prefixes of different width tries" in {
      val key = 0xfa2b3c4d
      val keyLength = 32

      // try for all trie widths of 1 to 32
      for (width <- List(1, 2, 4, 8, 16)) {
        val trie = new Trie(width, 0)
        // println("width = " + width)

        for (i <- width to keyLength by width) {
          trie.insert(key, i, i) mustEqual false
          trie.insert(key, i, i) mustEqual true
          if (i > 1) {
            //perturb to have more than 1 children of a node
            trie.insert(0, i, "%d:dummy".format(i)) mustEqual false
          }
          trie.exists(key, i) mustEqual (true, i)
          trie.exists(key ^ (1 << 31), i)._1 mustEqual false

          // println("trie after inserting length = %d :".format(i))
          // println(trie)
          trie.maxDepth() mustEqual i
          // what was inserted must be returned
          trie.search(key, i) mustEqual (i, i)
          // search of this perturbed key should catch the key of length (i - width) previous value
          trie.search(key, i - 1) mustEqual (i - width, i - width)
          var childrenAlongPath = ""
          for (n <- width to i by width) {
            childrenAlongPath += "|" + ( (key >>> (32 - n)) & ((1<<width)-1) )
          }
          trie.getItems() mustContain (childrenAlongPath, i, i)
        }
        // println("After inserting all keylengths in trie of width = %d : ".format(width) + trie)
        // println("trie's root node: " + trie.root)
        // println("All the trie's keys:" + trie.getItems())

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
            trie.insert(p, len, info) mustEqual (if (itn == 1) false else true)
            numPrefixes += 1
          }
          // println("[len = %d] INSERTED %d prefixes".format(len, numPrefixes))
        }
      }
      trie.getItems().size mustEqual numPrefixes
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
        // println("[len = %d] Search finished ".format(len))
      }
    }

    "hashKey of various keys and lengths" in {

      val key = new Array[Byte](3)
      key(0) = 0xab.toByte
      key(1) = 0xcd.toByte
      key(2) = 0xe3.toByte
      
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 1)) mustEqual "0000,0001,8000,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 2)) mustEqual "0000,0002,8000,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 3)) mustEqual "0000,0003,a000,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 4)) mustEqual "0000,0004,a000,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 5)) mustEqual "0000,0005,a800,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 6)) mustEqual "0000,0006,a800,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 7)) mustEqual "0000,0007,aa00,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 8)) mustEqual "0000,0008,ab00,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 9)) mustEqual "0000,0009,ab80,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 10)) mustEqual "0000,000a,abc0,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 16)) mustEqual "0000,0010,abcd,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 20)) mustEqual "0000,0014,abcd,e000,"
      Trie.unpackHashKeyIntoHexString(Trie.hashKey(key, 24)) mustEqual "0000,0018,abcd,e300,"
    }

    "inserting a node at odd level can then be found" in {
      val testDepth = 4
      var testTrie = new Trie(testDepth, "hello")
      val key = 0xa1b2c3d4
      val greeting = "deeper_hello"
      val keyLen = 6
      val isDup = testTrie.insert(key, keyLen, greeting)
      isDup mustEqual false
      // println("after adding 0x%x of length %d: testTrie is:".format(key, keyLen))
      // println(testTrie)

      testTrie.search(key, 4) mustEqual (0, testTrie.root.info)
      testTrie.search(key, 8) mustEqual (keyLen, greeting)
      testTrie.search(key, 7) mustEqual (keyLen, greeting)
      testTrie.search(0xafb2c3d4, 8) mustEqual (0, testTrie.root.info)
      testTrie.maxDepth() mustEqual 8
      testTrie.exists(key, keyLen) mustEqual (true, greeting)
      testTrie.getItems() mustEqual List(
        ("|10|3",keyLen, greeting), 
        ("|10|2",keyLen, greeting), 
        ("|10|1",keyLen, greeting), 
        ("|10|0",keyLen, greeting), 
        ("", 0, "hello"))
    }



  }
}

