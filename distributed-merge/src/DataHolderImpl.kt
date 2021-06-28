import system.DataHolderEnvironment

class DataHolderImpl<T : Comparable<T>>(
    private val keys: List<T>,
    private val dataHolderEnvironment: DataHolderEnvironment
) : DataHolder<T> {
    private var prevKeysCount : Int = -1
    private var keyCount : Int = 0


    override fun checkpoint() {
        prevKeysCount = keyCount
    }

    override fun rollBack() {
        keyCount = prevKeysCount
    }

    override fun getBatch(): List<T> {
        val counter = dataHolderEnvironment.batchSize
        var givenKeys = -1
        val  res  = ArrayList<T>()
        for (i in 0..counter-1) {
            if (i + keyCount >= keys.size) {
                givenKeys = i
                break
            }
            res.add(keys[i + keyCount])
        }
        if (givenKeys == -1){
            givenKeys = counter
        }
        keyCount += givenKeys
        return res
    }
}