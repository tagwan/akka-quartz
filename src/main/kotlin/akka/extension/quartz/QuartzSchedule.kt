package akka.extension.quartz

import org.quartz.*
import java.util.*

class QuartzSchedule(
    val name: String?,
    val description: String?,
    val expression: CronExpression?,
    val timezone: TimeZone?, //The name of the optional exclusion calendar to use.
    //NOTE: This formerly was "calendars" but that functionality has since been removed as Quartz never supported more
    //than one calendar anyways.
    private val calendar: String?
) {

    private var schedule: ScheduleBuilder<CronTrigger>

    constructor(
        name: String?,
        description: String?,
        expression: CronExpression?,
        timezone: TimeZone?,
        calendar: String?,
        schedule: ScheduleBuilder<CronTrigger>
    ) : this(name, description, expression, timezone, calendar) {
        this.schedule = schedule
    }

    init {
        schedule = CronScheduleBuilder.cronSchedule(expression).inTimeZone(timezone)
    }

    /**
     * Utility method that builds a trigger with the data this schedule contains, given a name.
     * Job association can happen separately at schedule time.
     *
     * @param name The name of the job / schedule.
     * @param futureDate The Optional earliest date at which the job may fire.
     * @return The new trigger instance.
     */
    fun buildTrigger(name: String, futureDate: Date?): Trigger {
        val partialTriggerBuilder: TriggerBuilder<*> = TriggerBuilder.newTrigger()
            .withIdentity(name + "_Trigger")
            .withDescription(description)
            .withSchedule(schedule)
        var triggerBuilder: TriggerBuilder<*>
        triggerBuilder = if (futureDate != null) {
            partialTriggerBuilder.startAt(futureDate)
        } else {
            partialTriggerBuilder.startNow()
        }
        if (calendar != null) {
            triggerBuilder = triggerBuilder.modifiedByCalendar(calendar)
        }
        return triggerBuilder.build()
    }
}