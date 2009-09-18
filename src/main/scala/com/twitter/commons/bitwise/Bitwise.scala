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

import net.lag.logging.Logger
import scala.collection.mutable


// A bunch of bit/byte manipulation utility functions

object Bitwise {

  private val log = Logger.get("Bitwise")

  // convert a Byte to an unsigned Byte 0.255 (stored as an Int)
  private def byte2Unsigned(x: Byte): Int = x & 255

  def convertIPAddressToArrayByte(addr: Int): Array[Byte] = convertIntToArrayByte(addr)
  def convertIntToArrayByte(addr: Int): Array[Byte] = Array( ((addr >> 24) & 255).toByte, ((addr >> 16) & 255).toByte, ((addr >> 8) & 255).toByte, ((addr >> 0) & 255).toByte)

  def convertArrayByteToIPAddr(ar: Array[Byte]): Int = convertArrayByteToInt(ar)
  def convArrayByteToIPString(byteArr: Array[Byte]): String = "%d.%d.%d.%d".format(byteArr(0) & 255, byteArr(1) & 255, byteArr(2) & 255, byteArr(3) & 255)


  // written procedural style for speed
  def convertArrayByteToInt(ar: Array[Byte]): Int = {
    var i: Int = 0
    var addr: Int = 0
    while (i < 4) {
      val v: Int = if (i < ar.length) (byte2Unsigned(ar(i))) else 0
      addr = (addr << 8) | v
      i += 1
    }
    addr
  }


  /**
   * Form a hashkey from data bytes, where data is of length dataLenBits
   * dataLenBits must be >= 1 and could be any positive value otherwise
   * the returned hashKey is a String of Chars packing in efficiently
   * dataLenBits and the significant data.
   *
   * This implements a general purpose pack function that packs an array of bytes of varying length
   * into a String.
   *
   * The output is used as a key into the oddPrefixes hashtable.
   */
  def hashKey(data: Array[Byte], dataLenBits: Int): String = {
    if (dataLenBits == 0) return "\0\0"
    assert(dataLenBits > 0)

    // last index in Array containing any meaningful data
    val lastIndex = (dataLenBits - 1) >> 3

    // zero out all the bits from data(lastIndx) that are not part of the data
    val lastByteVal: Int = byte2Unsigned(data(lastIndex)) & (~((1 << (7 - ((dataLenBits - 1) & 7))) - 1))

    // pack everything into an array of Chars: each Char is a 2byte entity keeping 0..(2^15-1)
    // 2 chars (4 bytes) for keyLenBits, ceil((lastIndx+1)/2) for data bytes. The latter is at most
    // lastIndx/2 + 1 bytes. +1 for safety :-)
    var st = new StringBuilder(2 + lastIndex/2 + 2)

    // First 2 Chars are the most significant (MS) 2 bytes and the LS 2 bytes respectively
    st.append((dataLenBits >> 16).toChar)
    st.append((dataLenBits & 0xff).toChar)

    // Pack the data bytes: each pair of successive bytes in one Char
    // take all pairs up to at most (lastIndx-2, lastIndx-1), i.e., NOT lastIndx
    var index = 0
    while (index < lastIndex-1) {
      st.append(((byte2Unsigned(data(index)) << 8) | byte2Unsigned(data(index + 1))).toChar)
      index += 2
    }
    val lastVal = if (index == lastIndex) { // data(lastIndex - 1) has already been consumed
      lastByteVal << 8
    } else {
      (byte2Unsigned(data(lastIndex - 1)) << 8) | lastByteVal
    }
    st.append(lastVal.toChar)
    st.toString
  }

  // opposite of hashKey
  def recoverDataFromHashKey(str: String): (Array[Byte], Int) = {
    var ab = new mutable.ArrayBuffer[Byte]()
    val chs = str.toCharArray()
    log.trace("chs= %s", chs.mkString(","))
    val len = ((chs(0).toInt) << 8) |  chs(1).toInt
    var indx = 2
    var lenAgg = 0
    while (lenAgg < len) {
      val i = chs(indx).toInt
      ab += (i >> 8).toByte
      lenAgg += 8
      if (lenAgg < len) {
        ab += (i & 0xff).toByte
        indx += 1
        lenAgg += 8
      }
    }
    (ab.toArray, len)
  }


  /**
   * Converts a hash key into a easily viewable hexadecimal string.
   */
  def unpackHashKeyIntoHexString(str: String) : String = {
    var sb = new StringBuilder
    var chs = str.toCharArray()
    chs.foreach { c =>
      val i = c.toInt
      sb.append("%02x".format(i >> 8))
      sb.append("%02x".format(i & 0xff))
      sb.append(",")
    }
    sb.toString
  }

  def hexChar(i: Int): Char = (if (i < 10) ('0' + i) else ('a' + i - 10)).toChar

  def byteArrayToHexString(bytes: Array[Byte]): String = {
    var s = new StringBuilder(2 * bytes.length)
    bytes.foreach { b =>
      val i: Int = b & 255 //convert to unsigned integer from 0..255
      s.append(hexChar(i >> 4))
      s.append(hexChar(i & 0xf))
    }
    s.toString
  }

  def prettyPrintBinaryStr(orig: String): String = ("" /: (orig.getBytes.map { b => if (b>32 && b<127) b.toChar else "\\x%02x".format(b & 255) })) (_ + _)

  /**
   * Extract 'numBits' number of bits from 'key' (which is an Array[Byte]) starting at 'startBitPos'.
   * Expect numBits < 32 for reliable results
   *
   * Examples: startBitPos=0, numBits = 8 returns key(0)
   *           startBitPos=0, numBits = 4 returns lsb 4 bits of key(0)
   *           startBitPos=1, numBits = 7 returns lsb 7 bits of key(0)
   *           startBitPos=2, numBits = 8 returns (lsb 6 bits of key(0), msb 2 bits of key(1) )
   *           startBitPos=2, numBits = 16 returns (lsb 6 bits of key(0), recursive call)
   */
  def extractIntFromArrayOfBytes(key: Array[Byte], startBitPos: Int, numBits: Int): Int =
    getIntFromArrayOfBytes(0, key, startBitPos, numBits)

  private def getIntFromArrayOfBytes(accum: Int, key: Array[Byte], startBitPos: Int, numBits: Int): Int = {
    val startIndex = startBitPos >> 3
    val startOff = startBitPos & 7
    val endOff = startOff + numBits
    val numBitsThisIndex = (8 min endOff) - startOff
    var nextAccum = (accum << numBitsThisIndex) | ((byte2Unsigned(key(startIndex)) >> (8 - (startOff + numBitsThisIndex))) & ( (1 << numBitsThisIndex) - 1))
    if ((endOff <= 8) || (startIndex >= key.length-1)) {
      // Take portion of the byte at key(startIndx)
      nextAccum
    } else {
      // accumulate numBitsThisIndx number of bits of the byte at key(startIndx)
      getIntFromArrayOfBytes(nextAccum, key,
        (startIndex + 1) << 3,  // start of next byte
        numBits - numBitsThisIndex)
    }
  }

  // Convert an Array of Int, where each element of the array represents an
  // Integer of range 0..(1<<width-1), to an array of bytes. This function is used
  // to recover the original key given an array of child#s from root
  // to the trienode representing the key.
  def convArrayOfInt2Byte(ai: Array[Int], width: Int): Array[Byte] = {
    // repeatedly get 8 bits worth of data
    val len = (ai.length * width + 7)/8 // ceil(x/8) = ceil((x+7)/8)
    val byArr = new Array[Byte](len)
    var bitCursor = 0
    var indx = 0
    while (indx < len) {
      byArr(indx) = extractByte(ai, width, bitCursor)
      bitCursor += 8
      indx += 1
    }
    byArr
  }

  // get 'numBits' number of bits starting from 'startBitPos' from an integer keeping
  // a value of width=frmWidthInBits i.e., ranging from 0 until (1<<fromWidthInBits - 1)
  // e.g.,
  // getBitsFromInt(10, 8, 0, 8) = 10
  // getBitsFromInt(10, 8, 4, 4) = 10
  // getBitsFromInt(10, 8, 5, 3) = 5
  // getBitsFromInt(10, 8, 6, 2) = 2
  // getBitsFromInt(5, 3, 0, 2) = 2
  def getBitsFromInt(frm: Int, frmWidthInBits: Int, startBitPos: Int, numBits: Int): Int = {
    var v = frm >> (frmWidthInBits - (startBitPos + numBits))
    v = v & ((1 << numBits) - 1)
    v
  }


  // extract a byte from an array of Ints of width numBitsEachElement stored in 'arr',
  // starting from startBitPos.
  def extractByte(arr: Array[Int], numBitsEachElement: Int, startBitPos: Int): Byte = extractSomeBits(arr, numBitsEachElement, startBitPos, 8).toByte

  // extract 'numBitsToExtract' number of bits. Must be < 32, else there will be unpredictable results
  def extractSomeBits(arr: Array[Int], numBitsEachElement: Int, startBitPos: Int, numBitsToExtract: Int): Int = {
    val startIndx = startBitPos/numBitsEachElement
    val endIndx = (startBitPos + numBitsToExtract)/numBitsEachElement min arr.length
    val startOff = startBitPos % numBitsEachElement
    val endOff = startOff + numBitsToExtract
    val numBitsThisIndx = (numBitsEachElement min endOff) - startOff

    // take a portion from [startIndx], all from [startIndx+1] till and including [endIndx-1] and
    // then a portion of [endIndx]
    var ret = getBitsFromInt(arr(startIndx), numBitsEachElement, startOff, numBitsThisIndx)
    var off = numBitsToExtract - numBitsThisIndx
    ret = ret << off // these are actually msb
    var indx = startIndx+1
    while (indx < endIndx) {
      off -= numBitsEachElement
      ret |= arr(indx) << off
      indx += 1
    }
    log.trace("startIndx=%d, endIndx=%d, startOff=%d, endOff=%d, numBitsThisIndx=%d, ret=%d, off=%d",
      startIndx, endIndx, startOff, endOff, numBitsThisIndx, ret, off)
    if (endIndx > startIndx && endIndx < arr.length) {
      // last indx
      val numBitsLastIndx = endOff % numBitsEachElement
      assert(off == numBitsLastIndx)
      if (numBitsLastIndx > 0)
        ret |= getBitsFromInt(arr(endIndx), numBitsEachElement, 0, numBitsLastIndx)
    }
    ret
  }


}
