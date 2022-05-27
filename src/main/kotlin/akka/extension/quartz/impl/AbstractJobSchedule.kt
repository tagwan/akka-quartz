package akka.extension.quartz.impl

import akka.actor.ExtendedActorSystem
import akka.extension.quartz.internal.CalendarUtil
import com.typesafe.config.Config
import org.quartz.Calendar
import org.quartz.Scheduler
import org.quartz.SchedulerException
import org.quartz.impl.DirectSchedulerFactory
import org.quartz.spi.JobStore
import org.quartz.spi.ThreadPool
import java.util.*

abstract class AbstractJobSchedule(
    protected val system: ExtendedActorSystem
) {

    protected val schedulerName: String
    protected val config: Config

    // Timezone to use unless specified otherwise
    var defaultTimezone: TimeZone
    var scheduler: Scheduler? = null

    init {
        schedulerName = "QuartzScheduler~" + system.name()
        config = system.settings().config().getConfig("akka.quartz").root().toConfig()
        defaultTimezone = TimeZone.getTimeZone(config.getString("defaultTimezone"))
        //this.initialize();
    }

    abstract fun newThreadPoolInstance(): ThreadPool
    abstract fun newJobStoreInstance(): JobStore

    protected fun initialize() {
        try {
            initScheduler()
            startScheduler()
            initCalendars()
        } catch (e: SchedulerException) {
            e.printStackTrace()
        }
    }

    @Throws(SchedulerException::class)
    private fun initScheduler() {
        val threadPool = newThreadPoolInstance()

        // TODO - Make this potentially configurable,  but for now we don't want persistable jobs.
        val jobStore = newJobStoreInstance()
        DirectSchedulerFactory.getInstance().createScheduler(
            schedulerName,
            system.name(),  /* todo - will this clash by quartz' rules? */
            threadPool,
            jobStore
        )
        scheduler = DirectSchedulerFactory.getInstance().getScheduler(schedulerName)
        //log.debug("Initialized a Quartz Scheduler '{}'", scheduler);
        system.registerOnTermination {
            val s = scheduler
            //log.info("Shutting down Quartz Scheduler with ActorSystem Termination (Any jobs awaiting completion will end as well, as actors are ending)...");
            try {
                s?.shutdown(false)
            } catch (e: SchedulerException) {
                e.printStackTrace()
            }
        }
    }

    @Throws(SchedulerException::class)
    protected abstract fun startScheduler()

    @Throws(SchedulerException::class)
    private fun initCalendars() {
        val calendarMap: Map<String, Calendar> = CalendarUtil.getCalendarMap(
            config, defaultTimezone
        )
        calendarMap.forEach { (name: String?, calendar: Calendar?) ->
            //log.info("Configuring Calendar '{}'", name);
            try {
                scheduler!!.addCalendar(name, calendar, true, true)
            } catch (e: SchedulerException) {
                e.printStackTrace()
            }
        }
        //log.info("Initialized calendars: " + scheduler.getCalendarNames());
    }
}