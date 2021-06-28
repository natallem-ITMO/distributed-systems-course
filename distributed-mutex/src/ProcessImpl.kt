package mutex

/**
 * Distributed mutual exclusion implementation.
 * All functions are called from the single main thread.
 *
 * @author Наталья Лемешкова
 */
class ProcessImpl(private val env: Environment) : Process {
    private var inCS = false // are we in critical section?
    private var reqCS = false // are we in critical section?
    private val edges = IntArray(env.nProcesses + 1) { x -> if (env.processId < x) 1 else 2 } // time of last REQ message
    private val waitingFork = BooleanArray(env.nProcesses + 1) { false } // time of last REQ message

    /*
    0 - fork is clean
    1 - fork is dirty
    2 - fork is not mine
     */
    override fun onMessage(srcId: Int, message: Message) {
        message.parse {
            val type = readEnum<MsgType>()
            when (type) {
                MsgType.REQ -> {
                    when (edges[srcId]) {
                        0 -> {
                            if (reqCS || inCS)
                                waitingFork[srcId] = true
                            else {
                                edges[srcId] = 2
                                env.send(srcId) {
                                    writeEnum(MsgType.OK)
                                }
                            }
                        }
                        1 -> {
                            edges[srcId] = 2
                            env.send(srcId) {
                                writeEnum(MsgType.OK)
                            }
                            if (reqCS) {
                                env.send(srcId) {
                                    writeEnum(MsgType.REQ)
                                }
                            }
                        }
                        2 -> {
                            env.send(srcId) {
                                writeEnum(MsgType.OK)
                            }
                        }
                    }
                }
                MsgType.OK -> {
                    edges[srcId] = 0
                    checkCSEnter();
                }
            }
        }
    }

    private fun checkCSEnter() {
        if (!reqCS || inCS) return // did not request CS, do nothing
        val curID = env.processId
        for (i in 1..env.nProcesses) {
            if (i != curID && edges[i] == 2) {
                return;
            }
        }
        for (i in 1..env.nProcesses) {
            if (i != curID) {
                edges[i] = 0;
            }
        }
        inCS = true
        reqCS = false;
        env.locked()
    }

    override fun onLockRequest() {
//        println("hello " + env.processId);
        check(!reqCS) { "Lock was already requested" }
        check(!inCS) { "Lock was already given" }
        reqCS = true
        for (i in 1..env.nProcesses) {
            if (i != env.processId && edges[i] == 2) {
                env.send(i) {
                    writeEnum(MsgType.REQ)
                }
            }
        }
        checkCSEnter();
    }

    override fun onUnlockRequest() {
        check(inCS) { "We are not in critical section" }
        inCS = false;
        env.unlocked()
        for (i in 1..env.nProcesses) {
            if (i != env.processId) {
                if (waitingFork[i]) {
                    waitingFork[i] = false
                    edges[i] = 2
                    env.send(i) {
                        writeEnum(MsgType.OK)
                    }
                } else {
                    edges[i] = 1
                }
            }
        }
        checkCSEnter();
    }

    enum class MsgType { REQ, OK }
}
