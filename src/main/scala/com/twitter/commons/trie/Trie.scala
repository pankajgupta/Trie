/** Copyright 2009 Twitter, Inc. */
package com.twitter.commons.ratelimiter
import net.lag.logging.Logger
import scala.collection.mutable

class Trie(val width: Int, val defaultInfo: Any) {
  import Trie._

  val root: TrieNode = new TrieNode(width)
  if (defaultInfo != null) root.insert(null, 0, defaultInfo, 0)
  private val log = Logger.get

  // This hashmap only contains prefixes whose lengths are not multiples of
  // width. The key is a string formed by concatenating len=length of the prefix as
  // the first character followed by len number of characters
  val oddPrefixes = new mutable.HashMap[String, Any]()

  def this(width: Int) = this(width, null)

  private def isOddLength(len: Int): Boolean = ((len % width) != 0)


  /**
   * @param key the key we are searching for
   * @param maxDepth the number of bits of the key we will use for traversal (how deep we'll look)
   *
   * Returns a tuple of depth of longest (deepest) matching trie node that contains a non-null
   * Info as well as this Info (if no such node exists, returns (0,null) )
   */
  def search(key: Array[Byte], maxDepth: Int): (Int, Any) = {
    val rem = (maxDepth % width)
    if (rem != 0) {
      var d = 0
      while (d < rem) {
        oddPrefixes.get(hashKey(key, maxDepth - d)) match {
          case Some(info) => return (maxDepth - d, info)
          case None =>  // fall through
        }
        d += 1
      }
    }
    val rv = traverseOnePath(key, maxDepth, false, root, 0, root.info, 0)
    (rv._4, rv._3)
  }

  def search(addr: Int, depth: Int): Any = search(convertIPAddressToArrayByte(addr), depth)

  def exists(key: Array[Byte], keyLen: Int): (Boolean, Any) = {
    val (depth, info) = search(key, keyLen)
    if (depth == keyLen)
      (true, info)
    else
      (false, info)
  }

  def exists(addr: Int, addrLen: Int): (Boolean, Any) = exists(convertIPAddressToArrayByte(addr), addrLen)

  /**
   * Travels down the trie, returning the last non-null Info (and the depth it was found)
   * The last non-null Info may not be the returning TrieNode's info.
   * Parameters key and keyLengthInBits remain the same in recursive invocations in the body
   */
  def traverseOnePath(key: Array[Byte], keyLengthInBits: Int, toInsertNodes: Boolean, currNode: TrieNode, startBitPos: Int, info: Any, infoDepth: Int): (TrieNode, Int, Any, Int) = {
    log.debug("in traverseOnePath(): startBitPos = %d , info = ".format(startBitPos) + info + " at depth = %d".format(infoDepth))
    if (keyLengthInBits < (startBitPos + width)) {
      return (currNode, startBitPos, info, infoDepth)
    }
    if (currNode.next == null) {
      if (toInsertNodes) {
        currNode.next = new Array[TrieNode](1 << width)
      } else {
        return (currNode, startBitPos, info, infoDepth)
      }
    }

    val currKeyPortion = getKeyPortion(0, key, startBitPos, width)
    var nextNode = currNode.next(currKeyPortion)

    var lastGoodInfo = info
    var lastGoodInfoDepth = infoDepth
    if (nextNode != null && nextNode.info != null) {
      lastGoodInfo = nextNode.info // override the accumulated info with the one found here
      lastGoodInfoDepth = startBitPos + nextNode.infoRelativeLen
    }
    if ((nextNode == null) && toInsertNodes) {
      currNode.next(currKeyPortion) = new TrieNode(width)
      nextNode = currNode.next(currKeyPortion)
    }

    if (nextNode != null) {
      traverseOnePath(key, keyLengthInBits, toInsertNodes, nextNode, startBitPos + width, lastGoodInfo, lastGoodInfoDepth)
    } else {
      (currNode, startBitPos + width, lastGoodInfo, lastGoodInfoDepth)
    }
  }

  /**
   * @param key to insert
   * @param keyLengthInBits depth at which to insert the key
   * @param info associated with the key
   *
   * Returns true if the key to insert already exists in the Trie. If it does,
   * the existing info is overwritten with the new info.
   */
  def insert(key: Array[Byte], keyLengthInBits: Int, info: Any): Boolean = {
    if (isOddLength(keyLengthInBits)) {
      oddPrefixes.put(hashKey(key, keyLengthInBits), info) match {
        case Some(existingInfo) => return true
        case None =>  // fall through
      }
    }
    val (node, startBitPosRem, _, _) = traverseOnePath(key, keyLengthInBits, true, root, 0, root.info, 0)
    val isDup = node.insert(key, keyLengthInBits, info, startBitPosRem)
    if (isDup)
      log.warning("attempting to overwrite key: %s at depth: %d with info: %s".format(key, keyLengthInBits, info))
    isDup
  }


  def insert(addr: Int, addrLengthInBits: Int, info: Any): Boolean = insert(convertIPAddressToArrayByte(addr), addrLengthInBits, info)

  // All the keys present in the trie and their info
  // In the form of list of (key, info), where each key is a
  // comma separated list of child numbers leading up to the trie node representing the key
  def getItems(): List[(String, Int, Any)] = root.getItemsSubtree()

  override def toString = "Trie width:%d depth:%d\nROOT:%s".format(width, maxDepth(), root.toString(0, true))

  def maxDepth(): Int = root.maxDepthSubtree()

}

/**
 * @param width number of bits we will use from the search key to go from one level to the next when searching
 * @param info the information we're storing at the leaf.
 */
class TrieNode(val width: Int) {
  var next: Array[TrieNode] = null // Is of size (1 << width)
  var info: Any = null

  // represents the length of info relative to this node's level
  // 0 implies no info present
  // 'width' implies the info is of a prefix at this level, 
  // 'width-1' is that of a prefix one bit shorter than this node's level
  var infoRelativeLen: Byte = 0

  override def toString = {
    toString(0, false)
  }

  def toString(level: Int, toRecurse: Boolean): String = {
    // For each level: print the elements of the array by name
    var nextDescr = "null"
    var childrenDescr = ""
    if (next != null) {
      val nonEmptyChildren = next.indices filter (next(_) != null)
      nextDescr = nonEmptyChildren.map("%x".format(_)).mkString("[", "|", "]")
      if (toRecurse) {
        childrenDescr = nonEmptyChildren.map( indx => ("  "*(level+1) + "Level=%d : Child# %x: ".format(level+1, indx) + next(indx).toString(level + 1, true))).mkString("\n")
      } else {
        childrenDescr = nonEmptyChildren.map( indx => ("  "*(level+1) + "Level=%d : Child# %x: ".format(level+1, indx))).mkString("\n")
      }
    }
    "next: " + nextDescr + ", infoRelativeLen:" + infoRelativeLen + ", info: " + info + "\n" + childrenDescr
  }

  def maxDepthSubtree(): Int = depthFirstApplySubtree( (_, _, thisLevel: Int) => thisLevel * width, (x: Int, y: Int) => x max y, "", 0)

  def getItemsSubtree(): List[(String, Int, Any)] = {
    depthFirstApplySubtree[List[(String, Int, Any)]](
      (nd: TrieNode, path: String, level: Int) => {
        if (nd != null && nd.info != null) List((path, (level-1)*width + nd.infoRelativeLen, nd.info)) else Nil
      },
      (sofar: List[(String, Int, Any)], y: List[(String, Int, Any)]) => y ::: sofar,
      "", 0)
  }

  // Traverse the subtree rooted at this node in depth-first order starting from this node
  // For every trienode encountered, apply f
  // Also compose the results of application of f
  // path = Comma delimited list of children numbers leading up to this node
  def depthFirstApplySubtree[T](f: (TrieNode, String, Int) => T, compose: (T, T) => T, path: String, nodeLevel: Int): T = {
    var rslt = f(this, path, nodeLevel)
    if (next != null) {
      for (childNum <- 0 until next.size) {
        val nd = next(childNum)
        val childPath = path + "|" + childNum.toString
        rslt = compose(rslt, if (nd == null) f(null, childPath, nodeLevel + 1) else nd.depthFirstApplySubtree(f, compose, childPath, nodeLevel + 1))
      }
    }
    rslt
  }

  def insertOdd(rlen: Int, keyInfo: Any): Int = {
    if (this.infoRelativeLen < rlen) {
      this.infoRelativeLen = rlen.toByte
      this.info = keyInfo
      1
    } else {
      0
    }
  }


  def insert(key: Array[Byte], keyLengthInBits: Int, keyInfo: Any, startBitPosRem: Int): Boolean = {
    if (startBitPosRem != keyLengthInBits) {
      val remLen = keyLengthInBits - startBitPosRem 
      assert(remLen < width)
      if (this.next == null) {
        this.next = new Array[TrieNode](1 << width)
      }
      val remBits = Trie.getKeyPortion(0, key, startBitPosRem, remLen) 
      var indx = remBits << (width - remLen)
      val maxIndx = (remBits + 1) << (width - remLen)
      var numChildrenAffected = 0
      while (indx < maxIndx) {
        if (this.next(indx) == null) {
          this.next(indx) = new TrieNode(width)
        }
        numChildrenAffected += this.next(indx).insertOdd(remLen, keyInfo)
        indx += 1
      }
      false
    } else {
      val isDuplicate: Boolean = (info != null)
      this.info = keyInfo
      this.infoRelativeLen = width.toByte
      isDuplicate
    }
  }

}

object Trie {
  // convert a Byte to Unsigned
  private def byte2Unsigned(x: Byte): Int = x & 255

  private def convertIPAddressToArrayByte(addr: Int): Array[Byte] = Array( ((addr >> 24) & 255).toByte, ((addr >> 16) & 255).toByte, ((addr >> 8) & 255).toByte, ((addr >> 0) & 255).toByte)

  // form a hashkey from data bytes, where data is of length dataLenBits
  // dataLenBits must be >= 1 and could be any positive value otherwise
  // the returned hashKey is a String of Chars packing in efficiently
  // dataLenBits and the significant data
  def hashKey(data: Array[Byte], dataLenBits: Int): String = {
    assert(dataLenBits > 0)

    // last indx in Array containing any meaningful data
    val lastIndx = (dataLenBits - 1) >> 3

    // zero out all the bits from data(lastIndx) that are not part of the data
    val lastByteVal: Int = byte2Unsigned(data(lastIndx)) & (~(( 1 << (7 - ((dataLenBits - 1) & 7))) - 1))

    // pack everything into an array of Chars: each Char is a 2byte entity keeping 0..(2^15-1)
    // 2 chars (4 bytes) for keyLenBits, ceil((lastIndx+1)/2) for data bytes. The latter is at most
    // lastIndx/2 + 1 bytes. +1 for safety :-)
    var st = new StringBuilder(2 + lastIndx/2 + 2)

    // First 2 Chars are the most significant (MS) 2 bytes and the LS 2 bytes respectively
    st.append((dataLenBits >> 16).toChar)
    st.append((dataLenBits & 0xff).toChar)

    // Pack the data bytes: each pair of successive bytes in one Char
    // take all pairs up to at most (lastIndx-2, lastIndx-1), i.e., NOT lastIndx
    var indx = 0
    while (indx < lastIndx-1) {
      st.append( ( (byte2Unsigned(data(indx)) << 8) | byte2Unsigned(data(indx+1))).toChar )
      indx += 2
    }
    val lastVal = if (indx == lastIndx) { // data(lastIndx-1) has already been consumed
      lastByteVal << 8
    } else {
      (byte2Unsigned(data(lastIndx-1)) << 8) | lastByteVal
    }
    st.append(lastVal.toChar)
    st.toString
  }

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

  // Get the key portion starting from 'startBitPos' stretching for numBits to the right
  // Examples: startBitPos=0, numBits = 8 returns key(0)
  //           startBitPos=1, numBits = 7 returns the lsb 7 bits of key(0)
  //           startBitPos=2, numBits = 8 returns (lsb 6 bits of key(0), msb 2 bits of key(1) )
  //           startBitPos=2, numBits = 16 returns (lsb 6 bits of key(0), recursive call)
  def getKeyPortion(accum: Int, key: Array[Byte], startBitPos: Int, numBits: Int): Int = {
    val startIndx = startBitPos >> 3
    val startOff = startBitPos & 7
    val endOff = startOff + numBits
    val numBitsThisIndx = (8 min endOff) - startOff
    var nextAccum = (accum << numBitsThisIndx) | ( (byte2Unsigned(key(startIndx)) >> (8 - (startOff + numBitsThisIndx))) & ( (1 << numBitsThisIndx) - 1) )
    if (endOff <= 8) {
      // Take portion of the byte at key(startIndx)
      nextAccum
    } else {
      // accumulate numBitsThisIndx number of bits of the byte at key(startIndx)
      getKeyPortion(nextAccum, key,
        (startIndx + 1) << 3,  // start of next byte
        numBits - numBitsThisIndx)
    }
  }

}

