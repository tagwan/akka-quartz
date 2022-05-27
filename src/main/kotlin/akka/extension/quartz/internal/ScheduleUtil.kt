package akka.extension.quartz.internal

import akka.extension.quartz.QuartzSchedule
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigValue
import org.quartz.CronExpression
import java.text.ParseException
import java.util.*

/**
 * This is really about triggers - as the "job" is roughly defined in the code that
 * refers to the trigger.
 *
 *
 * I call them Schedules to get people not thinking about Quartz in Quartz terms (mutable jobs, persistent state)
 *
 *
 * All jobs "start" immediately.
 */
object ScheduleUtil {
    // timezone (parseable) [optional, defaults to UTC]
    // calendars = list of calendar names that "modify" this schedule
    // description = an optional description of the job [string] [optional]
    // expression = cron expression complying to Quartz' Cron Expression rules.
    // TODO - Misfire Handling
    fun getScheduleMap(config: Config, defaultTimezone: TimeZone?): Map<String, QuartzSchedule> {
        val map: HashMap<String, QuartzSchedule> = HashMap<String, QuartzSchedule>()
        /** The extra toMap call is because the asScala gives us a mutable map...  */
        config.getConfig("schedules").root().forEach { name: String, value: ConfigValue ->
            val childCfg = value.atKey(name)
            map[name] = parseSchedule(name, childCfg.getConfig(name), defaultTimezone)
        }
        return map
    }

    fun parseSchedule(name: String, config: Config, defaultTimezone: TimeZone?): QuartzSchedule {
        // parse common attributes
        val timezone: TimeZone?
        timezone = try {
            TimeZone.getTimeZone(config.getString("timezone")) // todo - this is bad, as Java silently swaps the timezone if it doesn't match...
        } catch (e: ConfigException.Missing) {
            defaultTimezone
        }
        var calendar: String? = null
        var desc: String? = ""
        try {
            calendar =
                config.getString("timezone") // TODO - does Quartz validate for us that a calendar referenced is valid/invalid?
        } catch (e: ConfigException.Missing) {
        }
        try {
            desc = config.getString("description")
        } catch (e: ConfigException.Missing) {
        }
        return parseCronSchedule(name, desc, config, timezone, calendar)
    }

    fun parseCronSchedule(
        name: String,
        desc: String?,
        config: Config,
        tz: TimeZone?,
        calendar: String?
    ): QuartzSchedule {
        var expression: String? = null
        expression = try {
            config.getString("expression")
        } catch (e: ConfigException) {
            throw IllegalArgumentException(
                "Invalid or Missing Configuration entry 'expression' for Cron Schedule "
                        + name + ". You must provide a valid Quartz CronExpression."
            )
        }
        val expr: CronExpression
        expr = try {
            CronExpression(expression)
        } catch (e: ParseException) {
            throw IllegalArgumentException("Invalid 'expression' for Cron Schedule '$expression'. Failed to validate CronExpression.")
        }
        return QuartzSchedule(name, desc, expr, tz, calendar)
    }
}