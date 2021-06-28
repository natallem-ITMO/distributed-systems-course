import system.MergerEnvironment
import java.util.*
import kotlin.collections.HashMap
import kotlin.math.sign


class MergerImpl<T : Comparable<T>>(
    private val mergerEnvironment: MergerEnvironment<T>, private val
    prevStepBatches: Map<Int, List<T>>?
) : Merger<T> {

    val sorted = TreeMap<T, ArrayList<Int>>()
    var init = false

    val batches: HashMap<Int, List<T>> = HashMap()
    val batchesIndexes: HashMap<Int, Int> = HashMap() // index for first element that in sorted now

    override fun mergeStep(): T? {
        if (!init) {
            initialize()
        }
        if (sorted.isEmpty()) {
            return null
        }
        val first: T = sorted.firstKey()
        val list: ArrayList<Int> = sorted[first]!!
        val batchId = list.last()
        list.removeLast()
        if (list.isEmpty()) {
            sorted.remove(first)
        }
        readNext(batchId)
        return first
    }

    private fun readNext(batchId: Int) {
        batchesIndexes[batchId] =  batchesIndexes[batchId]!! + 1
        addToSortedValue(getKeyFromHolder(batchId), batchId)
    }

    private fun addToSortedValue(cur: T?, batchId: Int) {
        if (cur == null) return
        if (sorted.containsKey(cur)) {
            sorted[cur]!!.add(batchId)
        } else {
            val list = ArrayList<Int>()
            list.add(batchId)
            sorted.put(cur, list)
        }
    }

    private fun initialize() {
        init = true
        if (prevStepBatches == null) {
            for (i in 0 until mergerEnvironment.dataHoldersCount) {
                val cur = mergerEnvironment.requestBatch(i)
                if (cur.size != 0) {
                    batches[i] = cur
                    batchesIndexes[i] = 0
                }
            }
        } else {
            prevStepBatches.forEach { (x, y) ->
                batches[x] = y
                batchesIndexes[x] = 0
            }
        }
        batches.forEach { (x, _) ->
            addToSortedValue(getKeyFromHolder(x),x)
        }

    }

    private fun getKeyFromHolder(i: Int): T? {
        val curIndex: Int = batchesIndexes[i]!!
        if (curIndex >= batches[i]!!.size) {
            batches[i] = mergerEnvironment.requestBatch(i)
            if (batches[i]!!.isEmpty()) {
                batches.remove(i)
                batchesIndexes.remove(i)
                return null
            } else {
                batchesIndexes[i] = 0
            }
        }
        return batches[i]!![batchesIndexes[i]!!]
    }



    override fun getRemainingBatches(): Map<Int, List<T>> {
        val res: HashMap<Int, List<T>> = HashMap()
        batches.forEach{ (id, _) ->
            if (getKeyFromHolder(id) != null){
                res.put(id, getRestOfList(id))
            }
        }
        return res
    }

    private fun getRestOfList(id: Int): List<T> {
        val res : ArrayList<T> = ArrayList()
        for (i in batchesIndexes[id]!! until  batches[id]!!.size){
            res.add(batches[id]!![i])
        }
        return res
    }
}