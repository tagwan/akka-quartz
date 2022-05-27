package akka.extension.quartz.impl

import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.ExtendedActorSystem
import akka.event.EventStream
import akka.extension.quartz.QuartzSchedule
import akka.extension.quartz.internal.ScheduleUtil
import org.quartz.*
import org.quartz.core.jmx.JobDataMapSupport
import scala.collection.concurrent.TrieMap
import java.text.ParseException
import java.util.*
import java.util.function.BiConsumer

abstract class AbstractQuartzExtension(system: ExtendedActorSystem?, private val jobClass: Class<out Job?>) :
    AbstractJobSchedule(system!!) {
    /**
     * Parses job and trigger configurations, preparing them for any code request of a matching job.
     * In our world, jobs and triggers are essentially 'merged'  - our scheduler is built around triggers
     * and jobs are basically 'idiot' programs who fire off messages.
     */
    var schedules = TrieMap<String, QuartzSchedule>()
    var runningJobs = TrieMap<String, JobKey>()

    @Throws(SchedulerException::class)
    override fun startScheduler() {
        val scheduleMap: Map<String, QuartzSchedule> = ScheduleUtil.getScheduleMap(
            config,
            defaultTimezone
        )
        scheduleMap.forEach { (key, value) ->
            schedules.put(
                key,
                value
            )
        }
        //log.debug("Configured Schedules: {}", schedules);
        scheduler!!.start()
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Puts the Scheduler in 'standby' mode, temporarily halting firing of triggers.
     * Resumable by running 'start'
     */
    @Throws(SchedulerException::class)
    fun standby() {
        scheduler!!.standby()
    }

    @get:Throws(SchedulerException::class)
    val isInStandbyMode: Boolean
        get() = scheduler!!.isInStandbyMode

    @get:Throws(SchedulerException::class)
    val isStarted: Boolean
        get() = scheduler!!.isStarted

    @Throws(SchedulerException::class)
    fun start(): Boolean {
        return if (isStarted) {
            //log.warn("Cannot start scheduler, already started.");
            false
        } else {
            scheduler!!.start()
            true
        }
    }

    @Throws(SchedulerException::class)
    fun nextTrigger(name: String?): Date {
        while (true) {
            val jobKey = runningJobs.get(name)
            if (jobKey.nonEmpty()) {
                val headOption = scheduler!!.getTriggersOfJob(jobKey.get() as JobKey).stream().findFirst()
                if (headOption.isPresent) {
                    return headOption.get().nextFireTime
                }
            }
        }
    }

    @Throws(SchedulerException::class)
    fun suspendAll() {
        //log.info("Suspending all Quartz jobs.");
        scheduler!!.pauseAll()
    }

    /**
     * Shutdown the scheduler manually. The scheduler cannot be re-started.
     *
     * @param waitForJobsToComplete wait for jobs to complete? default to false
     */
    @JvmOverloads
    @Throws(SchedulerException::class)
    fun shutdown(waitForJobsToComplete: Boolean? = false) {
        scheduler!!.shutdown(waitForJobsToComplete!!)
    }

    /**
     * Attempts to suspend (pause) the given job
     *
     * @param name The name of the job, as defined in the schedule
     * @return Success or Failure in a Boolean
     */
    @Throws(SchedulerException::class)
    fun suspendJob(name: String?): Boolean {
        val option = runningJobs.get(name)
        return if (option.nonEmpty()) {
            val job = option.get() as JobKey
            //log.info("Suspending Quartz Job '{}'", name);
            scheduler!!.pauseJob(job)
            true
        } else {
            //log.warn("No running Job named '{}' found: Cannot suspend", name);
            false
        }
    }

    /**
     * Attempts to resume (un-pause) the given job
     *
     * @param name The name of the job, as defined in the schedule
     * @return Success or Failure in a Boolean
     */
    @Throws(SchedulerException::class)
    fun resumeJob(name: String?): Boolean {
        val option = runningJobs.get(name)
        return if (option.nonEmpty()) {
            val job = option.get() as JobKey
            //log.info("Resuming Quartz Job '{}'", name);
            scheduler!!.resumeJob(job)
            true
        } else {
            //log.warn("No running Job named '{}' found: Cannot resume", name);
            false
        }
    }

    /**
     * Unpauses all jobs in the scheduler
     */
    @Throws(SchedulerException::class)
    fun resumeAll() {
        //log.info("Resuming all Quartz jobs.");
        scheduler!!.resumeAll()
    }

    @Throws(SchedulerException::class)
    fun cancelJob(name: String?): Boolean {
        val option = runningJobs.get(name)
        return if (option.nonEmpty()) {
            val job = option.get() as JobKey
            //log.info("Cancelling Quartz Job '{}'", name);
            val result = scheduler!!.deleteJob(job)
            runningJobs.remove(name)
            result
        } else {
            //log.warn("No running Job named '{}' found: Cannot cancel", name);
            false
        }
    }

    /**
     * Creates job, associated triggers and corresponding schedule at once.
     *
     * @param name           The name of the job, as defined in the schedule
     * @param receiver       An ActorRef, who will be notified each time the schedule fires
     * @param msg            A message object, which will be sent to `receiver` each time the schedule fires
     * @param description    A string describing the purpose of the job
     * @param cronExpression A string with the cron-type expression
     * @param calendar       An optional calendar to use.
     * @param timezone       The time zone to use if different from default.
     * @return A date which indicates the first time the trigger will fire.
     */
    @JvmOverloads
    @Throws(SchedulerException::class)
    fun createJobSchedule(
        name: String, receiver: ActorRef, msg: Any, description: String?,
        cronExpression: String?, calendar: String?, timezone: TimeZone? = defaultTimezone
    ) {
        createSchedule(name, description, cronExpression, calendar, timezone)
        schedule(name, receiver, msg)
    }

    /**
     * Updates job, associated triggers and corresponding schedule at once.
     *
     * @param name           The name of the job, as defined in the schedule
     * @param receiver       An ActorRef, who will be notified each time the schedule fires
     * @param msg            A message object, which will be sent to `receiver` each time the schedule fires
     * @param description    A string describing the purpose of the job
     * @param cronExpression A string with the cron-type expression
     * @param calendar       An optional calendar to use.
     * @param timezone       The time zone to use if different from default.
     * @return A date which indicates the first time the trigger will fire.
     */
    @JvmOverloads
    @Throws(SchedulerException::class)
    fun updateJobSchedule(
        name: String, receiver: ActorRef,
        msg: Any, description: String?,
        cronExpression: String?, calendar: String?,
        timezone: TimeZone? = defaultTimezone
    ): Date {
        return rescheduleJob(name, receiver, msg, description, cronExpression, calendar, timezone)
    }

    /**
     * Deletes job, associated triggers and corresponding schedule at once.
     *
     *
     * Remark: Identical to `unscheduleJob`. Exists to provide consistent naming of related JobSchedule operations.
     *
     * @param name The name of the job, as defined in the schedule
     * @return Success or Failure in a Boolean
     */
    @Throws(SchedulerException::class)
    fun deleteJobSchedule(name: String?): Boolean {
        return unscheduleJob(name)
    }

    /**
     * Unschedule an existing schedule
     *
     *
     * Cancels the running job and all associated triggers and removes corresponding
     * schedule entry from internal schedules map.
     *
     * @param name The name of the job, as defined in the schedule
     * @return Success or Failure in a Boolean
     */
    @Throws(SchedulerException::class)
    fun unscheduleJob(name: String?): Boolean {
        val isJobCancelled = cancelJob(name)
        if (isJobCancelled) {
            removeSchedule(name)
        }
        return isJobCancelled
    }

    /**
     * Create a schedule programmatically (must still be scheduled by calling 'schedule')
     *
     * @param name           A String identifying the job
     * @param description    A string describing the purpose of the job
     * @param cronExpression A string with the cron-type expression
     * @param calendar       An optional calendar to use.
     */
    fun createSchedule(
        name: String,
        description: String?,
        cronExpression: String?,
        calendar: String?,
        timezone: TimeZone?
    ) {
        val expression: CronExpression
        expression = try {
            CronExpression(cronExpression)
        } catch (e: ParseException) {
            throw IllegalArgumentException(
                "Invalid 'expression' for Cron Schedule '\$name'. Failed to validate CronExpression.",
                e
            )
        }
        val schedule = QuartzSchedule(name, description, expression, timezone, calendar)
        require(
            !schedules.putIfAbsent(name, schedule).nonEmpty()
        ) { "A schedule with this name already exists: [$name]" }
    }

    /**
     * Reschedule a job
     *
     * @param name           A String identifying the job
     * @param receiver       An ActorRef, who will be notified each time the schedule fires
     * @param msg            A message object, which will be sent to `receiver` each time the schedule fires
     * @param description    A string describing the purpose of the job
     * @param cronExpression A string with the cron-type expression
     * @param calendar       An optional calendar to use.
     * @return A date which indicates the first time the trigger will fire.
     */
    @Throws(SchedulerException::class)
    fun rescheduleJob(
        name: String, receiver: ActorRef,
        msg: Any, description: String?,
        cronExpression: String?, calendar: String?, timezone: TimeZone?
    ): Date {
        cancelJob(name)
        removeSchedule(name)
        createSchedule(name, description, cronExpression, calendar, timezone)
        return scheduleInternal(name, receiver, msg, null)
    }

    private fun removeSchedule(name: String?) {
        schedules.remove(name)
    }

    /**
     * Schedule a job, whose named configuration must be available
     *
     * @param name     A String identifying the job, which must match configuration
     * @param receiver An ActorRef, who will be notified each time the schedule fires
     * @param msg      A message object, which will be sent to `receiver` each time the schedule fires
     * @return A date which indicates the first time the trigger will fire.
     */
    @Throws(SchedulerException::class)
    fun schedule(name: String, receiver: ActorRef, msg: Any): Date {
        return scheduleInternal(name, receiver, msg, null)
    }

    /**
     * Schedule a job, whose named configuration must be available
     *
     * @param name     A String identifying the job, which must match configuration
     * @param receiver An ActorSelection, who will be notified each time the schedule fires
     * @param msg      A message object, which will be sent to `receiver` each time the schedule fires
     * @return A date which indicates the first time the trigger will fire.
     */
    @Throws(SchedulerException::class)
    fun schedule(name: String, receiver: ActorSelection, msg: Any): Date {
        return scheduleInternal(name, receiver, msg, null)
    }

    /**
     * Schedule a job, whose named configuration must be available
     *
     * @param name     A String identifying the job, which must match configuration
     * @param receiver An EventStream, who will be published to each time the schedule fires
     * @param msg      A message object, which will be published to `receiver` each time the schedule fires
     * @return A date which indicates the first time the trigger will fire.
     */
    @Throws(SchedulerException::class)
    fun schedule(name: String, receiver: EventStream, msg: Any): Date {
        return scheduleInternal(name, receiver, msg, null)
    }

    /**
     * Schedule a job, whose named configuration must be available
     *
     * @param name     A String identifying the job, which must match configuration
     * @param receiver An ActorRef, who will be notified each time the schedule fires
     * @param msg      A message object, which will be sent to `receiver` each time the schedule fires
     * @return A date which indicates the first time the trigger will fire.
     */
    @Throws(SchedulerException::class)
    fun schedule(name: String, receiver: ActorRef, msg: Any, startDate: Date?): Date {
        return scheduleInternal(name, receiver, msg, startDate)
    }

    /**
     * Schedule a job, whose named configuration must be available
     *
     * @param name     A String identifying the job, which must match configuration
     * @param receiver An ActorSelection, who will be notified each time the schedule fires
     * @param msg      A message object, which will be sent to `receiver` each time the schedule fires
     * @return A date which indicates the first time the trigger will fire.
     */
    @Throws(SchedulerException::class)
    fun schedule(name: String, receiver: ActorSelection, msg: Any, startDate: Date?): Date {
        return scheduleInternal(name, receiver, msg, startDate)
    }

    /**
     * Schedule a job, whose named configuration must be available
     *
     * @param name     A String identifying the job, which must match configuration
     * @param receiver An EventStream, who will be published to each time the schedule fires
     * @param msg      A message object, which will be published to `receiver` each time the schedule fires
     * @return A date which indicates the first time the trigger will fire.
     */
    @Throws(SchedulerException::class)
    fun schedule(name: String, receiver: EventStream, msg: Any, startDate: Date): Date {
        return scheduleInternal(name, receiver, msg, startDate)
    }

    /**
     * Helper method for schedule because overloaded methods can't have default parameters.
     *
     * @param name      The name of the schedule / job.
     * @param receiver  The receiver of the job message. This must be either an ActorRef or an ActorSelection.
     * @param msg       The message to send to kick off the job.
     * @param startDate The optional date indicating the earliest time the job may fire.
     * @return A date which indicates the first time the trigger will fire.
     */
    @Throws(SchedulerException::class)
    protected fun scheduleInternal(name: String, receiver: Any, msg: Any, startDate: Date?): Date {
        val option = schedules.get(name)
        return if (option.nonEmpty()) {
            val schedule: QuartzSchedule = option.get() as QuartzSchedule
            scheduleJob(name, receiver, msg, startDate, schedule)
        } else {
            throw IllegalArgumentException(
                "No matching quartz configuration found for schedule '$name'"
            )
        }
    }

    @Throws(SchedulerException::class)
    private fun scheduleJob(
        name: String,
        receiver: Any,
        msg: Any,
        startDate: Date?,
        schedule: QuartzSchedule
    ): Date {
        //log.info("Setting up scheduled job '{}', with '{}'", name, scheduler);
        val jobDataMap: HashMap<String, Any> =  hashMapOf(
            "logBus" to system.eventStream(),
            "receiver" to receiver,
            "message" to msg
        )
        val jobData = JobDataMapSupport.newJobDataMap(jobDataMap)
        val job = JobBuilder.newJob(jobClass)
            .withIdentity(name + "_Job")
            .usingJobData(jobData)
            .withDescription(schedule.description)
            .build()

        //log.debug("Adding jobKey {} to runningJobs map.", job.getKey());
        runningJobs.put(name, job.key)
        val showdate = startDate ?: Date()
        //log.debug("Building Trigger with startDate '{}", showdate);
        val trigger: Trigger = schedule.buildTrigger(name, startDate)

        //log.debug("Scheduling Job '{}' and Trigger '{}'. Is Scheduler Running? {}", job, trigger, scheduler.isStarted());
        return scheduler!!.scheduleJob(job, trigger)
    }
}