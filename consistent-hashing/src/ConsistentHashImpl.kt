import kotlin.collections.HashMap
import kotlin.collections.HashSet

class ConsistentHashImpl<K> : ConsistentHash<K> {

    private val circle: MutableList<Int> = MutableList(0, { x -> x })
    private val vnodeShard = HashMap<Int, Shard>()
    private val shardPoints = HashMap<Shard, HashSet<Int>>()

    override fun getShardByKey(key: K): Shard {
        val curVnodeHash = getPrevHash(key.hashCode())
        return vnodeShard[curVnodeHash]!!
    }

    override fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>> {
        val resultMap: HashMap<Shard, HashSet<HashRange>> = HashMap<Shard, HashSet<HashRange>>()
        shardPoints.put(newShard, HashSet())
        if (shardPoints.size == 1) {
            val l = IntArray(vnodeHashes.size)
            var pos = 0
            vnodeHashes.forEach { x ->
                l[pos] = x
                ++pos
            }
            l.sort()

            l.forEach { x ->
                circle.add(x)
                vnodeShard[x] = newShard
                shardPoints[newShard]!!.add(x)
            }
        } else {
            vnodeHashes.forEach { curHash ->
                var insertPos = circle.binarySearch(curHash)
                insertPos = if (insertPos < 0) -(insertPos + 1) else insertPos
                val nextPos = if (insertPos == circle.size) 0 else insertPos
                val prevPos: Int = if (nextPos == 0) circle.size - 1 else nextPos - 1
                if (prevPos != -1) {
                    val toRemoveFrom: Shard = vnodeShard[circle[nextPos]]!!
                    var startFromHash: Int = circle[prevPos] + 1
                    if (toRemoveFrom != newShard) {
                        if (!resultMap.containsKey(toRemoveFrom)) {
                            resultMap[toRemoveFrom] = HashSet<HashRange>()
                        }
                        val curSetRange: HashSet<HashRange> = resultMap[toRemoveFrom]!!
                        var prev: HashRange? = null
                        curSetRange.forEach { x ->
                            if (x.rightBorder == startFromHash - 1) {
                                prev = x
                            }
                        }
                        if (prev != null){
                            startFromHash = prev!!.leftBorder
                            curSetRange.remove(prev)
                        }
                        curSetRange.add(HashRange(startFromHash, curHash))
                    }
                }
                addToListInPosition(insertPos, curHash)
                vnodeShard[curHash] = newShard
                shardPoints[newShard]!!.add(curHash)
            }
        }
        return resultMap
    }

    override fun removeShard(shard: Shard): Map<Shard, Set<HashRange>> {
        val resultMap: HashMap<Shard, HashSet<HashRange>> = HashMap()
        check(shardPoints.size != 1) { "shardPoints.size != 1" }
        val hashesOfShard: HashSet<Int> = shardPoints[shard]!!
        while (hashesOfShard.isNotEmpty()) {
            var toDeleteNumber = 1
            val firstHash = hashesOfShard.first()
            var posNext = getNextPos(circle.binarySearch(firstHash))
            var posPrev = getPrevPos(getPrevPos(posNext))
            while (vnodeShard[circle[posNext]] == shard) {
                ++toDeleteNumber
                posNext = getNextPos(posNext)
            }
            while (vnodeShard[circle[posPrev]] == shard) {
                ++toDeleteNumber
                posPrev = getPrevPos(posPrev)
            }
            val posLastCur = getPrevPos(posNext)
            var posFirstCur = getNextPos(posPrev)
            val beginRange = circle[posPrev] + 1
            val endRange = circle[posLastCur]
            val toRemoveFrom: Shard = vnodeShard[circle[posNext]]!!
            if (!resultMap.containsKey(toRemoveFrom)) {
                resultMap[toRemoveFrom] = HashSet<HashRange>()
            }
            val curSetRange: HashSet<HashRange> = resultMap[toRemoveFrom]!!
            curSetRange.add(HashRange(beginRange, endRange))
            for (i in 1..toDeleteNumber) {
                posFirstCur = if (posFirstCur == circle.size) 0 else posFirstCur
                deleteHash(posFirstCur, shard)
            }
        }
        return resultMap
    }

    private fun getNextPos(pos: Int): Int {
        var newPos = pos + 1
        newPos = if (newPos == circle.size) 0 else newPos
        return newPos
    }

    private fun getPrevPos(pos: Int): Int {
        var newPos = pos - 1
        newPos = if (newPos == -1) circle.size - 1 else newPos
        return newPos
    }

    private fun deleteHash(pos: Int, shard: Shard) {
        val hashValue = circle[pos]
        vnodeShard.remove(hashValue)
        shardPoints[shard]!!.remove(hashValue)
        deletePosition(pos)
    }

    private fun deletePosition(pos: Int) {
        var curPerm = pos
        while (curPerm < circle.size - 1) {
            circle[curPerm] = circle[curPerm + 1]
            curPerm++
        }
        circle.removeLast()
    }

    private fun addToListInPosition(pos: Int, value: Int) {
        circle.add(value)
        var curPerm = circle.size - 2
        while (curPerm >= pos) {
            circle[curPerm + 1] = circle[curPerm]
            --curPerm
        }
        circle[pos] = value
    }

    private fun getNextPosition(hash: Int): Int {
        var curPos = circle.binarySearch(hash)
        curPos = if (curPos < 0) -(curPos + 1) else curPos
        if (curPos == circle.size) {
            curPos = 0
        }
        return curPos
    }

    private fun getPrevHash(hash: Int): Int {
        val curPos = getNextPosition(hash)
        return circle[curPos]
    }

}