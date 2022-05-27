package akka.extension.quartz

import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.event.EventStream
import akka.extension.quartz.Message.RequireFireTime
import akka.extension.quartz.Message.WithFireTime
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException

/**
 * For Akka 2.5.x and Scala 2.11.x, 2.12.x, 2.13.x
 * @link https://github.com/enragedginger/akka-quartz-scheduler
 */
class SimpleActorMessageJob : Job {
    val jobType = "SimpleActorMessage"

    /**
     * These jobs are fundamentally ephemeral - a new Job is created
     * each time we trigger, and passed a context which contains, among
     * other things, a JobDataMap, which transfers mutable state
     * from one job trigger to another
     *
     * @throws JobExecutionException
     */
    @Throws(JobExecutionException::class)
    override fun execute(context: JobExecutionContext) {
        val dataMap = context.jobDetail.jobDataMap
        val key = context.jobDetail.key
        try {
            //LoggingBus logBus = (LoggingBus) dataMap.get("logBus");
            val receiver = dataMap["receiver"]
            var msg = dataMap["message"]
            if (msg is RequireFireTime) {
                msg = WithFireTime(
                    msg.msg,
                    context.scheduledFireTime,
                    context.previousFireTime,
                    context.nextFireTime
                )
            }
            if (receiver is ActorRef) {
                receiver.tell(msg, ActorRef.noSender())
            } else if (receiver is ActorSelection) {
                receiver.tell(msg, ActorRef.noSender())
            } else if (receiver is EventStream) {
                receiver.publish(msg)
            } else {
                throw JobExecutionException("receiver as not expected type, must be ActorRef or ActorSelection, was " + receiver!!.javaClass)
            }
        } catch (e: Exception) {
            // All exceptions thrown from a job, including Runtime, must be wrapped in a JobExcecutionException or Quartz ignores it
            if (e is JobExecutionException) {
                throw e
            } else {
                throw JobExecutionException(
                    "ERROR executing Job " + key.name + " : " + e.message,
                    e
                ) // todo - control refire?
            }
        }
    }
}