package akka.extension.quartz.impl

import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.extension.quartz.SimpleActorMessageJob
import org.quartz.simpl.RAMJobStore
import org.quartz.simpl.SimpleThreadPool
import org.quartz.spi.JobStore
import org.quartz.spi.ThreadPool

class SimpleQuartzExtension(system: ExtendedActorSystem) :
    AbstractQuartzExtension(system, SimpleActorMessageJob::class.java), Extension {

    // The # of threads in the pool
    var threadCount: Int
    var threadPriority: Int

    // Should the threads we create be daemonic? FYI Non-daemonic threads could make akka / jvm shutdown difficult
    var daemonThreads: Boolean

    init {
        threadCount = super.config.getInt("threadPool.threadCount")
        threadPriority = super.config.getInt("threadPool.threadPriority")
        daemonThreads = super.config.getBoolean("threadPool.daemonThreads")
        super.initialize()
    }


    override fun newThreadPoolInstance(): ThreadPool {
        require(threadCount >= 1) { "Quartz Thread Count (akka.quartz.threadPool.threadCount) must be a positive integer." }
        require(!(threadPriority < 1 || threadPriority > 10)) { "Quartz Thread Priority (akka.quartz.threadPool.threadPriority) must be a positive integer between 1 (lowest) and 10 (highest)." }
        val threadPool = SimpleThreadPool(threadCount, threadPriority)
        threadPool.threadNamePrefix = "AKKA_QURTZ_"
        threadPool.isMakeThreadsDaemons = daemonThreads
        return threadPool
    }

    override fun newJobStoreInstance(): JobStore {
        return RAMJobStore()
    }
}