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

package com.twitter.commons.bitwise

import org.specs._
import net.lag.logging.Logger
import org.specs.matcher.Matcher
import scala.util.Random

object BitwiseSpec extends Specification {
  import Bitwise._
  private val log = Logger.get("BitwiseSpec")

  case class matchElements[T](a: Array[T]) extends Matcher[Array[T]] {
    def apply(v: => Array[T]) = {
      val vstr = v.mkString(",")
      val astr = a.mkString(",")
      (vstr == astr, "Elements in arrays matched ", "Elements in arrays did not match. Expected: %s  Actual: %s".format(vstr, astr))
    }
  }

  "Bitwise" should {

    "convert between IP address and Array of Bytes" >> {
      val egs: List[(Int, Array[Byte], String)] = List(
        (0x12345678, Array(0x12, 0x34, 0x56, 0x78), "18.52.86.120"),
        (0x123456f8, Array(0x12, 0x34, 0x56, 0xf8.toByte), "18.52.86.248"),
        (0x1, Array(0, 0, 0, 0x1), "0.0.0.1"),
        (0x345fed, Array(0, 0x34, 0x5f, 0xed.toByte), "0.52.95.237")
      )
      egs foreach { case (addr, byArr, ipStr) =>
        convertIPAddressToArrayByte(addr) must matchElements[Byte](byArr)
        convertArrayByteToIPAddr(byArr) mustEqual addr
        convArrayByteToIPString(byArr) mustEqual ipStr
      }
    }

    "hashKey of various keys and lengths" >> {

      val key = new Array[Byte](3)
      key(0) = 0xab.toByte
      key(1) = 0xcd.toByte
      key(2) = 0xe3.toByte

      unpackHashKeyIntoHexString(hashKey(key, 1)) mustEqual "0000,0001,8000,"
      unpackHashKeyIntoHexString(hashKey(key, 2)) mustEqual "0000,0002,8000,"
      unpackHashKeyIntoHexString(hashKey(key, 3)) mustEqual "0000,0003,a000,"
      unpackHashKeyIntoHexString(hashKey(key, 4)) mustEqual "0000,0004,a000,"
      unpackHashKeyIntoHexString(hashKey(key, 5)) mustEqual "0000,0005,a800,"
      unpackHashKeyIntoHexString(hashKey(key, 6)) mustEqual "0000,0006,a800,"
      unpackHashKeyIntoHexString(hashKey(key, 7)) mustEqual "0000,0007,aa00,"
      unpackHashKeyIntoHexString(hashKey(key, 8)) mustEqual "0000,0008,ab00,"
      unpackHashKeyIntoHexString(hashKey(key, 9)) mustEqual "0000,0009,ab80,"
      unpackHashKeyIntoHexString(hashKey(key, 10)) mustEqual "0000,000a,abc0,"
      unpackHashKeyIntoHexString(hashKey(key, 16)) mustEqual "0000,0010,abcd,"
      unpackHashKeyIntoHexString(hashKey(key, 20)) mustEqual "0000,0014,abcd,e000,"
      unpackHashKeyIntoHexString(hashKey(key, 24)) mustEqual "0000,0018,abcd,e300,"
    }

    "binary/hex string manipulation should be good" >> {
      hexChar(4) mustEqual '4'
      hexChar(0) mustEqual '0'
      hexChar(10) mustEqual 'a'
      hexChar(15) mustEqual 'f'
      val ba: Array[Byte] = Array(0, 12, 33, 6, 127, -128, -1)
      byteArrayToHexString(ba) mustEqual "000c21067f80ff"
    }

    "getBitsFromInt should work" >> {
      getBitsFromInt(10, 8, 0, 8) mustEqual 10
      getBitsFromInt(10, 8, 4, 4) mustEqual 10
      getBitsFromInt(10, 8, 5, 3) mustEqual 2
      getBitsFromInt(14, 8, 5, 3) mustEqual 6
      getBitsFromInt(10, 8, 6, 2) mustEqual 2
      getBitsFromInt(5, 3, 0, 2) mustEqual 2

      val v = 0x123456d8
      for (i <- 0 to 24 by 8) {
        getBitsFromInt(v, 32, i, 8) mustEqual ((v >> (24 - i)) & 255)
      }

      for (i <- 0 to 28 by 4) {
        getBitsFromInt(v, 32, i, 4) mustEqual ((v >> (28 - i)) & 15)
      }

      for (i <- 0 to 29 by 3) {
        getBitsFromInt(v, 32, i, 3) mustEqual ((v >> (29 - i)) & 7)
      }
    }

    "conversion of array of int to array of byte" >> {
      val intArr  = new Array[Int](255)
      intArr.indices foreach { i => intArr(i) = i}
      val byArr = intArr map {e => e.toByte}
      convArrayOfInt2Byte(intArr, 8) must matchElements[Byte](byArr)
      convArrayOfInt2Byte(intArr, 16) must matchElements[Byte](intArr flatMap {e => Array(0.toByte, e.toByte) })
      val intArr4 = intArr flatMap {e => Array(e>>4, e&0xf)}
      convArrayOfInt2Byte(intArr4, 4) must matchElements[Byte](byArr)

      val b1: Array[Byte] = Array(0x1b)
      convArrayOfInt2Byte(Array(0,1,2,3), 2) must matchElements(b1)
      val b2: Array[Byte] = Array(0x5, 0x30)
      convArrayOfInt2Byte(Array(0,1,2,3), 3) must matchElements(b2)
      val b3: Array[Byte] = Array(0x5d, 0xf3.toByte)
      convArrayOfInt2Byte(Array(0,1,0,1,1,1,0,1,1,1,1,1,0,0,1,1), 1) must matchElements(b3)

    }

    "extractIntFromArrayOfBytes should work" >> {
      val byArr: Array[Byte] = Array(0x12, 0x34, 0x56, 0x78, 0x9a.toByte, 0xbc.toByte, 0xde.toByte)
      // all these crazy (& 255) converts the byte to an integer
      byArr.indices foreach { indx =>
        extractIntFromArrayOfBytes(byArr, indx*8, 8) mustEqual (byArr(indx) & 255)
        extractIntFromArrayOfBytes(byArr, indx*8, 4) mustEqual ((byArr(indx) & 255) >> 4)
        extractIntFromArrayOfBytes(byArr, 4 + indx*8, 4) mustEqual (byArr(indx) & 0xf)
        extractIntFromArrayOfBytes(byArr, 4 + indx*8, 2) mustEqual ( (byArr(indx)>>2) & 0x3)
        extractIntFromArrayOfBytes(byArr, indx*8, 16) mustEqual
          (if (indx == (byArr.length - 1)) ((byArr(indx) & 255)) else
            ( ( (byArr(indx) & 255) <<8) | ( byArr(indx+1) & 255) ) )
      }
    }

    "extractByte from an array of Ints should work" >> {
      val intArr: Array[Int] = Array(0x12, 0x34, 0x12345678, 0x9abcdef1)

      extractByte(intArr, 8, 0) mustEqual 0x12.toByte
      extractByte(intArr, 8, 4) mustEqual 0x23.toByte
      extractByte(intArr, 8, 6) mustEqual 0x8d.toByte
      extractByte(intArr, 32, 64) mustEqual 0x12.toByte
      extractByte(intArr, 32, 68) mustEqual 0x23.toByte
      extractByte(intArr, 32, 96) mustEqual 0x9a.toByte
    }

    "extractSomeBits should work" >> {
      val intArr: Array[Int] = Array(0x12, 0x34, 0x12345678, 0x9abcdef1)
      extractSomeBits(intArr, 8, 0, 4) mustEqual 0x1
      extractSomeBits(intArr, 8, 0, 7) mustEqual (0x12 >> 1)
    }

    // go Random baby
    "extractSomeBits from Array[Int] should be identical to extractIntFromArrayOfBytes on random data" >> {
      val intArr = new Array[Int](256)
      intArr.indices foreach { i => intArr(i) = i }
      val byArr: Array[Byte] = intArr map { e => e.toByte }
      val rnd = new Random(0x1234f)

      var startBitPos = 0
      var numBits = 0
      for (i <- 1 to 1000) {
        startBitPos = rnd.nextInt(256)
        numBits = rnd.nextInt(32)
        // println("startBitPos=%d, numBits=%d".format(startBitPos, numBits))
        extractIntFromArrayOfBytes(byArr, startBitPos, numBits) mustEqual extractSomeBits(intArr, 8, startBitPos, numBits)
      }
    }

  }
}

