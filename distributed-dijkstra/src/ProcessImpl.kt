package dijkstra

import dijkstra.messages.*
import dijkstra.system.environment.Environment

class ProcessImpl(private val env: Environment) : Process {

    var dist: Long = Long.MAX_VALUE
    var childCnt: Int = 0
    var balance: Int = 0
    var parentId: Int = env.pid
    var isInitial: Boolean = false

    override fun onMessage(srcId: Int, message: Message) {
        if (message is UpdateDistMessage) {
            val new_value = message.dist
            if (new_value < dist) {
                makeParent(srcId)
                dist = new_value
                broadcast()
                untouchedFromParent()
            }
            env.send(srcId, AckMessage)
        } else if (message is AddChildMessage) {
            childCnt++
        } else if (message is DeleteChildMessage) {
            childCnt--
            untouchedFromParent()
        } else if (message is AckMessage) {
            --balance
            untouchedFromParent()
        } else {
            check(false) { "Unexpected else branch" }
        }

    }

    private fun broadcast() {
        for ((k, v) in env.neighbours) {
            sendMessage(k, UpdateDistMessage(v + dist))
        }
    }

    private fun makeParent(newParentId: Int) {
        if (parentId == env.pid) {
            parentId = newParentId
            env.send(parentId, AddChildMessage)
        }
    }

    private fun untouchedFromParent() {
        if (balance == 0 && childCnt == 0) {
            if (isInitial) {
                env.finishExecution();
            } else {
                env.send(parentId, DeleteChildMessage)
                parentId = env.pid;
            }
        }
    }

    private fun sendMessage(dstId: Int, message: Message) {
        balance++
        env.send(dstId, message);
    }


    override fun getDistance(): Long? {
        return if (dist == Long.MAX_VALUE) null else dist
    }

    override fun startComputation() {
        isInitial = true
        dist = 0
        if (env.neighbours.isEmpty()){
            env.finishExecution()
        } else {
            broadcast()
        }
    }
}