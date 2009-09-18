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

import net.lag.logging.Logger
import scala.collection.mutable

import net.lag.logging.Logger
import scala.collection.mutable
import com.twitter.commons.bitwise._

/**
 * Generic Trie data structure stores prefixes of varying lengths along with any information.
 *
 * Supports the following operations:
 *  - Insert
 *  - Delete
 *  - Existence
 *  - Longest prefix match
 *
 * The value of width can be tuned for the trade-off between the depth of the tree and the number of
 * internal nodes. The depth of the tree determines the search speed and internal nodes determines the storage.
 * So this is a classic speed/storage trade-off. For example, when storing IP prefixes, width can be a full
 * byte or a nibble, 1 bit, etc, and this length is encoded simply as an Int.
 *
 * Information is a generic element that can be used to store associated data. It's of type Any (as opposed to
 * Option[T]) to allow nulls to be used for performance.
 *
 * An example using IP prefixes: An IP prefix is a value and a length, such as 10.2.0.0/16 where the first two octets
 * are the ones under consideration.
 *
 * This trie can be used for storing and searching IP prefixes. It can store a prefix at the appropriate level
 * based on the length given. For example, 10.2.0.0/16 in a 8-width trie would be stored at the 2nd level,
 * with 10 being the 1st level TrieNnode and 2 being the 2nd level TrieNode.
 *
 * @param width The radix of the trie (i.e., the number of bits used from a key at each level)
 * @param rootInfo The default information stored at the root level.
 */
class Trie(val width: Int, private val rootInfo: Any) {
  private val log = Logger.get

  private val root: TrieNode = new TrieNode(width)
  if (rootInfo != null) root.insert(null, 0, rootInfo, 0)

  /**
   * This hashmap only contains prefixes whose lengths are not divisible by width, i.e., (prefix_length % width != 0)
   * The key is a string formed by concatenating [len:value] of the prefix.
   * Depending on how many elements are in oddPrefixes, you might decide to change your width. Ideally this
   * hashtable will be small for your use. If it grows large, consider changing your width.
   */
  val oddPrefixes = new mutable.HashMap[String, Any]()

  /**
   * Alternate constructor that provides a null default info.
   */
  def this(width: Int) = this(width, null)

  private def isOddLength(len: Int): Boolean = ((len % width) != 0)

  def clear(): Unit = {
    oddPrefixes.clear()
    root.clear()
  }

  /**
   * Find the longest odd length prefix (and its info) between begLen and endLen (both lengths included)
   * Return length of loLen-1 if not found any prefix
   */
  def findLongestPrefixBackwards(key: Array[Byte], hiLen: Int, loLen: Int): (Any, Int) = {
    var len = hiLen
    while (len >= loLen) {
        oddPrefixes.get(Bitwise.hashKey(key, len)) match {
          case Some(info) => return (info, len)
          case None =>  // fall through
        }
        len -= 1
      }
    return (null, len)
  }


  /**
   * Returns a tuple of depth of longest (deepest) matching trie node that contains a non-null
   * Info as well as this Info (if no such node exists, returns (0,null) )
   *
   * @param key the key we are searching for
   * @param maxDepth the number of bits of the key we will use for traversal (how deep we'll look)
   */
  def search(key: Array[Byte], maxDepth: Int): (Int, Any) = {
    val rem = (maxDepth % width)
    if (rem != 0) {
      var d = 0
      while (d < rem) {
        oddPrefixes.get(Bitwise.hashKey(key, maxDepth - d)) match {
          case Some(info) => return (maxDepth - d, info)
          case None =>  // fall through
        }
        d += 1
      }
    }
    val rv = traverseOnePathAlongKey(key, maxDepth)
    (rv._4, rv._3)
  }

  /**
   * Convenience method for easy searching of an IP prefix.
   *
   * @param value the IP prefix value to search for.
   * @param length the IP prefix length.
   */
  def search(value: Int, length: Int): Any = search(Bitwise.convertIPAddressToArrayByte(value), length)

  /**
   * Checks to see if a key exists and there's info at that level.
   *
   * @param key the key to search for.
   * @param keyLen how many bits the key is.
   */
  def exists(key: Array[Byte], keyLen: Int): Option[Any] = {
    val (depth, info) = search(key, keyLen)
    if (depth == keyLen) Some(info) else None
  }

  /**
   * Tests for the existence of an IP prefix.
   *
   * @param value the IP prefix to search for.
   * @param length the IP prefix length.
   */
  def exists(value: Int, length: Int): Option[Any] = exists(Bitwise.convertIPAddressToArrayByte(value), length)


  /**
   * Public wrapper around traverseOnePath
   */
  def traverseOnePathAlongKey(key: Array[Byte], keyLengthInBits: Int): (List[(TrieNode, Int)], Int, Any, Int) = traverseOnePath(key, keyLengthInBits, false, List((root, 0)), 0, root.info, 0)


  /**
   * Helper function:
   * Travels down the trie, returning the last non-null Info (and the depth it was found)
   * The last non-null Info may not be the returning TrieNode's info.
   * Parameters key and keyLengthInBits remain the same in recursive invocations in the body
   * Accumulates the TrieNodes encountered in its traversal into a stack expressed
   * as a List[(TrieNode, Int)] (first list element is the deepest trie node, and the
   * last list element is the root of the trie, also the second part of the tuple is the
   * index of the child that this node is of its parent)
   */
  private def traverseOnePath(key: Array[Byte], keyLengthInBits: Int, toInsertNodes: Boolean,
    traversedNodesStack: List[(TrieNode, Int)], startBitPos: Int, info: Any, infoDepth: Int): (List[(TrieNode, Int)], Int, Any, Int) = {
    log.trace("in traverseOnePath(): startBitPos = %d , info = %s at depth = %d", startBitPos, info, infoDepth)
    if (keyLengthInBits < (startBitPos + width)) {
      return (traversedNodesStack, startBitPos, info, infoDepth)
    }
    val currNode = traversedNodesStack.head._1
    if (currNode.next == null) {
      if (toInsertNodes) {
        currNode.next = new Array[TrieNode](1 << width)
      } else {
        return (traversedNodesStack, startBitPos, info, infoDepth)
      }
    }

    val currKeyPortion = Bitwise.extractIntFromArrayOfBytes(key, startBitPos, width)
    var nextNode = currNode.next(currKeyPortion)
    log.trace("in traverseOnePath(): Next node at 0x%x", currKeyPortion)

    var lastGoodInfo = info
    var lastGoodInfoDepth = infoDepth
    if (nextNode != null && nextNode.info != null) {
      lastGoodInfo = nextNode.info // override the accumulated info with the one found here
      lastGoodInfoDepth = startBitPos + nextNode.infoRelativeLen
    }
    if (nextNode == null) {
      if (toInsertNodes) {
        currNode.next(currKeyPortion) = new TrieNode(width)
        nextNode = currNode.next(currKeyPortion)
      } else {
        return (traversedNodesStack, startBitPos, lastGoodInfo, lastGoodInfoDepth)
      }
    }

    if (nextNode != null) {
      traverseOnePath(key, keyLengthInBits, toInsertNodes, (nextNode, currKeyPortion) :: traversedNodesStack, startBitPos + width, lastGoodInfo, lastGoodInfoDepth)
    } else {
      (traversedNodesStack, startBitPos + width, lastGoodInfo, lastGoodInfoDepth)
    }
    /***
    if (nextNode == null) && toInsertNodes) {
      currNode.next(currKeyPortion) = new TrieNode(width)
      nextNode = currNode.next(currKeyPortion)
    }

    if (nextNode != null) {
      traverseOnePath(key, keyLengthInBits, toInsertNodes, (nextNode, currKeyPortion) :: traversedNodesStack, startBitPos + width, lastGoodInfo, lastGoodInfoDepth)
    } else {
      (traversedNodesStack, startBitPos + width, lastGoodInfo, lastGoodInfoDepth)
    }
    ****/
  }

  /**
   * @param key to insert
   * @param keyLengthInBits depth at which to insert the key
   * @param info associated with the key
   *
   * Returns None if the key-to-be-inserted did not already exist in the Trie.
   * and Some(existingInfo) if it did.
   */
  def insert(key: Array[Byte], keyLengthInBits: Int, info: Any): Option[Any] = {
    log.trace("Inserting key %s of length = %d bits", key.map("%x".format(_)).mkString("."), keyLengthInBits)
    if (isOddLength(keyLengthInBits)) {
      oddPrefixes.put(Bitwise.hashKey(key, keyLengthInBits), info) match {
        case Some(existingInfo) => return Some(existingInfo)
        case None =>  // fall through to rest of function
      }
    }
    val (nodeList, startBitPosRem, _, _) = traverseOnePath(key, keyLengthInBits, true, List((root, 0)), 0, root.info, 0)
    val res = (nodeList.head._1).insert(key, keyLengthInBits, info, startBitPosRem)
    if (res != None) {
      log.trace("attempting to overwrite key: %s at depth: %d with existingInfo:%s and newInfo: %s",
          key, keyLengthInBits, res.get, info)
    }
    res
  }


  /**
   * Convenience method for inserting an IP prefix.
   *
   * @param value the value of the IP prefix.
   * @param length the length of the IP prefix (in bits).
   * @param info information associated with the IP prefix.
   */
  def insert(value: Int, length: Int, info: Any): Option[Any] = insert(Bitwise.convertIPAddressToArrayByte(value), length, info)

  /**
   * Helper function that removes empty nodes from a given List of (TrieNode, Depth) pairs.
   */
  private def removeEmptyNodes(nodeChain: List[(TrieNode, Int)]): Unit = {
    var it = nodeChain.elements
    var m = it.next
    while (it.hasNext) {
      val nd = m._1
      val indx = m._2
      val nextElem = it.next
      if (nd.isRedundant()) {
        nextElem._1.removeChild(indx)
      } else return
      m = nextElem
    }
  }

  /**
   *
   * @param key bytes to delete
   * @param keyLengthInBits depth of key to delete
   *
   * Returns Some(existingInfo) if the key to delete exists and was deleted successfully. Otherwise, returns None
   */
  def delete(key: Array[Byte], keyLengthInBits: Int): Option[Any] = {
    log.trace("Deleting key %s of length = %d bits", key.map("%x".format(_)).mkString("."), keyLengthInBits)
    var existingInfo: Any = null
    if (isOddLength(keyLengthInBits)) {
      oddPrefixes.removeKey(Bitwise.hashKey(key, keyLengthInBits)) match {
        case Some(e) => existingInfo = e // fall through to rest of code
        case None => return None
      }
    }
    val (nodeList, startBitPosRem, _, _) = traverseOnePath(key, keyLengthInBits, false, List((root, 0)), 0, root.info, 0)
    log.trace("nodeList=%s, startBitPosRem=%d", nodeList.toString, startBitPosRem)
    val existingInfo2 = (nodeList.head._1).delete(key, keyLengthInBits, startBitPosRem, this)
    existingInfo2 match {
    case Some(e2) =>
      removeEmptyNodes(nodeList)
      Some(if (existingInfo != null) existingInfo else e2)
    case None => None
    }
  }

  /**
   * Convenience method for deleting an IP prefix.
   *
   * @param value the value of the IP prefix.
   * @param length the length of the IP prefix (in bits).
   */
  def delete(value: Int, length: Int): Option[Any] = delete(Bitwise.convertIPAddressToArrayByte(value), length)

  // FIXME: implement clear()

  /**
   * Returns all the trie nodes with non-null info as a List of Tuples of (Key, Level, Info)
   * Where each key is a '|' separated list of child numbers leading up to the trie node representing the key
   * For example, if one key 0x12/8 with info="I" is present in a trie of width=4,
   * return value will be List("|1|2", 8, "I")
   */
  def getNodesWithNonNullInfo(): List[(String, Int, Any)] = root.getNodesWithNonNullInfoSubtree(false)

  override def toString = "Trie width:%d depth:%d\nROOT:%s".format(width, maxDepth(), root.toString(0, true))

  def maxDepth(): Int = root.maxDepthSubtree()

  def numNodes(): Int = root.numChildrenSubtree()

  def size(): Int = root.numItemsSubtree() + oddPrefixes.size

  // returns elements in Trie in depth-first order (i.e., ones actually in trie nodes),
  // not oddPrefixes
  def elementsTrieNodesOnlyList(): List[(Array[Byte], Int, Any)] = root.elementsSubtreeTrieNodesOnly()

  def elements(): List[(Array[Byte], Int, Any)] = {
    val m: Map[String, Any] = root.elementsSubtreeMap()
    val keysSorted: List[String] = m.keys.toList sort((e1, e2) => (e1 compareTo e2) < 0)
    val lst = new mutable.ListBuffer[(Array[Byte], Int, Any)]
    keysSorted foreach { k =>
      val v = m(k)
      val (ab, len) = Bitwise.recoverDataFromHashKey(k)
      lst += (ab, len, v)
    }
    lst.toList
  }


  def defaultInfo(): Any = root.info
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
  // 'width-1' is that of a prefix one bit shorter than this node's level, and so on
  var infoRelativeLen: Byte = 0

  def clear() {
    next = null // hoping that Garbage Collection will take care of the lost pointers...
    info = null
    infoRelativeLen = 0
  }

  override def toString = {
    toString(0, false)
  }

  /**
   * Pretty-prints all the elements at a given level. Optionally recurses into their children.
   *
   * @param toRecurse
   */
  def toString(level: Int, toRecurse: Boolean): String = {
    // For each level: print the elements of the array by name
    var nextDescr = "null"
    var childrenDescr = ""
    if (next != null) {
      val nonEmptyChildren = next.indices filter (next(_) != null)
      nextDescr = nonEmptyChildren.map("%x".format(_)).mkString("[", "|", "]")
      if (toRecurse) {
        childrenDescr = nonEmptyChildren.map( indx => ("  "*(level+1) + "RelativeLevel=%d : Child# %x: ".format(level+1, indx) + next(indx).toString(level + 1, true))).mkString("\n")
      } else {
        childrenDescr = nonEmptyChildren.map( indx => ("  "*(level+1) + "RelativeLevel=%d : Child# %x: ".format(level+1, indx))).mkString("\n")
      }
    }
    "New Node: next: " + nextDescr + "; infoRelativeLen:" + infoRelativeLen + "; info: " + info + "\n" + childrenDescr
  }

  /**
   * Returns the maximum depth of any of the subtrees.
   */
  def maxDepthSubtree(): Int = depthFirstApplySubtree(
    (nd: TrieNode, _, thisLevel: Int) => if (nd == null) 0 else thisLevel * width,
    (x: Int, y: Int) => x max y,
    "",
    0)

  def numItemsSubtree(): Int = depthFirstApplySubtree(
    (nd, _, _) => if (nd == null || nd.info == null || (nd.infoRelativeLen < width)) 0 else 1,
    addInts _,
    "",
    0)

  /**
   * Returns the total number of children (i.e., those that are non-null)
   */
  def numChildrenSubtree(): Int = depthFirstApplySubtree(
    (nd, _, _) => if (nd == null) 0 else 1,
    addInts _,
    "",
    0)

  private def nodeIsNonNull(nd: TrieNode, path: String, level: Int) = (nd != null && nd.info != null)

  // returns elements in subtree in depth-first order (i.e., ones actually in trie nodes),
  // not oddPrefixes
  def elementsSubtreeTrieNodesOnly(): List[(Array[Byte], Int, Any)] =
    convPathString2ArrayByte(getNodesSubtree(nodeIsNonNull)) //getNodesWithNonNullInfoSubtree(true)

  def convPathString2ArrayByte(lst: List[(String, Int, Any)]): List[(Array[Byte], Int, Any)] = {
    lst map { e =>
      val lstStr = e._1
      // convert the path string (which is an '|' separated string of Ints(child#s)) to Array[Byte]
      val ab: Array[Int] = lstStr.split('|').drop(1).toArray map {x: String => x.toInt}
      (Bitwise.convArrayOfInt2Byte(ab, width), e._2, e._3)
    }
  }

  def elementsSubtreeMap(): Map[String, Any] = {
    depthFirstApplySubtree[Map[String, Any]](
      (nd: TrieNode, path: String, level: Int) => {
        if (nd != null && nd.info != null) {
          val lenBits = (level-1) * width + nd.infoRelativeLen
          val childNumsInPath: Array[Int] = path.split('|').drop(1).toArray.map {x: String => x.toInt}
          val data: Array[Byte] = Bitwise.convArrayOfInt2Byte(childNumsInPath, width)
          val hash = Bitwise.hashKey(data, lenBits)
          Map[String, Any](hash -> nd.info)
        } else {
          Map[String, Any]()
        }
      }, addMaps _, "", 0)
  }

  def addMaps(a: Map[String, Any], b: Map[String, Any]): Map[String, Any] = a ++ b

  def addInts(a: Int, b: Int): Int = a + b

  /**
   * Returns all the children structured as a List of their keys, depth, and information.
   * If onlyItems==true, further narrows down to original items, but only those items that
   * are NOT oddPrefixes
   */
  def getNodesWithNonNullInfoSubtree(onlyItems: Boolean): List[(String, Int, Any)] = {
    depthFirstApplySubtree[List[(String, Int, Any)]](
      (nd: TrieNode, path: String, level: Int) => {
        if (nd != null && nd.info != null && (!onlyItems || nd.infoRelativeLen == width))
          List((path, (level-1)*width + nd.infoRelativeLen, nd.info)) else Nil
      },
      (sofar: List[(String, Int, Any)], y: List[(String, Int, Any)]) => y ::: sofar,
      "", 0)
  }

  /**
   * Returns all the children nodes structured as a List of their keys, depth, and information.
   * Takes in a function 'f' that defines which nodes to include
   */
  def getNodesSubtree(f: (TrieNode, String, Int) => Boolean): List[(String, Int, Any)] = {
    depthFirstApplySubtree[List[(String, Int, Any)]](
      (nd: TrieNode, path: String, level: Int) => if (f(nd, path, level)) List((path, (level-1)*width + nd.infoRelativeLen, nd.info)) else Nil,
      (sofar: List[(String, Int, Any)], y: List[(String, Int, Any)]) => y ::: sofar,
      "", 0)
  }

  /**
   * Traverse the subtree rooted at this node in depth-first order starting from this node, apply f to every
   * TrieNode enountered, then compose all the results.
   *
   * @param f  a function to apply for every TrieNode encountered
   * @param compse a functio to compose to results of applying f to each TrieNode.
   * @param path = Comma delimited list of children numbers leading up to this node
   */
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

  /**
   * Inserts a key and and Info into the Trie.
   *
   * @param key the key to insert
   * @param keyLengthInBits how many bits the key is
   * @param keyInfo the information you want associated with the key
   * @param the start position of the subset of the key you want to use.
   *
   * Returns Some(existingInfo) if the key was already present and None otherwise
   */
  def insert(key: Array[Byte], keyLengthInBits: Int, keyInfo: Any, startBitPosRem: Int): Option[Any] = {
    if (startBitPosRem != keyLengthInBits) {
      val remLen = keyLengthInBits - startBitPosRem
      assert(remLen < width)
      if (this.next == null) {
        this.next = new Array[TrieNode](1 << width)
      }
      val remBits = Bitwise.extractIntFromArrayOfBytes(key, startBitPosRem, remLen)
      var indx = remBits << (width - remLen)
      val maxIndx = (remBits + 1) << (width - remLen)
      var numChildrenAffected = 0
      while (indx < maxIndx) {
        if (this.next(indx) == null) {
          this.next(indx) = new TrieNode(width)
        }
        val child = this.next(indx)
        if (child.infoRelativeLen < remLen) {
          child.infoRelativeLen = remLen.toByte
          child.info = keyInfo
          numChildrenAffected += 1
        }
        indx += 1
      }
      None
    } else {
      val isDuplicate: Boolean = (info != null)
      val existing = info
      this.info = keyInfo
      this.infoRelativeLen = width.toByte
      if (isDuplicate) Some(existing) else None
    }
  }


  /**
   * Deletes a given key from the given Trie using a portion of the key. The portion of the key to use is determined
   * by taking the startBitPosRem position of the key and going to the end of the key.
   *
   * @param key the key to delete
   * @param keyLengthInBits how long the key is in bits.
   * @param startBitPosRem the number of bits you want from the end of the key.
   * @param trie the trie to delete from.
   *
   * Returns None if the key never existed, and Some(existingInfo) otherwise
   */
  def delete(key: Array[Byte], keyLengthInBits: Int, startBitPosRem: Int, trie: Trie): Option[Any] = {
    if (startBitPosRem != keyLengthInBits) {
      val remLen = keyLengthInBits - startBitPosRem
      if (remLen >= width) {
        // did not find what we were looking for because otherwise we would have gone deeper in the Trie
        //log.trace("remLen = %d, width = %d. Not deleting.", remLen, width)
        return None
      }
      if (this.next == null) {
        return None
      }
      val remBits = Bitwise.extractIntFromArrayOfBytes(key, startBitPosRem, remLen)
      var indx = remBits << (width - remLen)
      val maxIndx = (remBits + 1) << (width - remLen)
      var existingInfo: Any = null
      var numChildrenAffected = 0
      var lpm: (Any, Int) = (null, -1) // -1 is used to indicate that lpm has not been computed yet
      while (indx < maxIndx) {
        val child = this.next(indx)
        assert(child != null)
        existingInfo = child.info
        assert(child.infoRelativeLen >= remLen)
        if (child.infoRelativeLen == remLen) {
          if (lpm._2 == -1) {
            // find the longest prefix less than this key, but only need to do it once
            // after the first time this is called, lpm._2 will be >= 1
            lpm = trie.findLongestPrefixBackwards(key, keyLengthInBits - 1, startBitPosRem + 1)
          }
          child.info = lpm._1
          child.infoRelativeLen = (lpm._2 - startBitPosRem).toByte
          // cleanup
          if (child.isRedundant()) this.removeChild(indx)
          numChildrenAffected += 1
        }
        indx += 1
      }
      Some(existingInfo)
    } else {
      val existingInfo = this.info
      if (this.info != null) {
        if (keyLengthInBits > 0) {
          val (i, oddLen) = trie.findLongestPrefixBackwards(key, keyLengthInBits - 1, keyLengthInBits - width + 1)
          this.info = i
          this.infoRelativeLen = (oddLen - startBitPosRem + width).toByte
        }
        Some(existingInfo)
      } else {
        None
      }
    }
  }

  /**
   * Returns whether this node is redundant i.e., is not needed to be present in the trie any longer. This
   * could happen because of deletions.
   */
  def isRedundant(): Boolean = (info == null && infoRelativeLen == 0 && (next == null || next.forall(_ == null)))

  /**
   * Removes the children of a given index.
   */
  def removeChild(index: Int): Unit = {
    next(index) = null
  }
}
