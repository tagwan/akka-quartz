package akka.extension.quartz.internal

import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigException.WrongType
import com.typesafe.config.ConfigValue
import org.quartz.CronExpression
import org.quartz.impl.calendar.*
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors

/**
 * Utility classes around the creation and configuration of Quartz Calendars.
 * All dates must be ISO-8601 compliant.
 */
object CalendarUtil {
    // TODO - Support a default / "Base" calendar.
    // type (of enum list)
    // timezone (parseable) [optional, defaults to UTC]
    // description (string)
    /* annual
     *   excludes a set of days of the year
     *   e.g. bank holidays which are on same date every year
     *   doesn't take year into account, but can't calculate moveable feasts.
     *   i.e. you can specify "christmas" (always december 25) and it will get every christmas,
     *   but not "Easter" (which is calculated based on the first full moon on or after the spring equinox)
     */
    // excludeDates (list of MM-DD dates)
    /* holiday
     *  excludes full specified day, with the year taken into account
     */
    // excludeDates (list of ISO-8601 dates, YYYY-MM-DD)
    /* daily
     *  excludes a specified time range each day. cannot cross daily boundaries.
     *  only one time range PER CALENDAR.
     */
    // exclude block {
    //    startTime = <TIME>
    //    endTime   = <TIME>
    // }
    // time in the format "HH:MM[:SS[:mmm]]" where:
    //    HH is the hour of the specified time. The hour should be specified using military (24-hour) time and must be in the range 0 to 23.
    //    MM is the minute of the specified time and must be in the range 0 to 59.
    //    SS is the second of the specified time and must be in the range 0 to 59.
    //    mmm is the millisecond of the specified time and must be in the range 0 to 999.
    //  items enclosed in brackets ('[', ']') are optional.
    // !! The time range starting time must be before the time range ending time.
    //    Note this means that a time range may not cross daily boundaries (10PM - 2AM) !!
    /* monthly
     *  excludes a set of days of the month.
     */
    // excludeDays (list of ints from 1-31)
    /* weekly
     *  Excludes a set of days of the week, by default excludes Saturday and Sunday
     */
    // excludeDays (list of ints from 1-7 where 1 is sunday, 7 is saturday)
    // excludeWeekends (boolean) By default TRUE, *overriden by excludeDays* (e.g. if you say this is true but exclude sunday, exclude wins)
    /* cron
     *  excludes the set of times expressed by a given [Quartz CronExpression](http://quartz-scheduler.org/api/2.1.7/org/quartz/CronExpression.html)
     *  Gets *one* expression set on it.
     *
     */
    // excludeExpression (Valid Quartz CronExpression)
    fun getCalendarMap(config: Config, defaultTimezone: TimeZone?): Map<String, org.quartz.Calendar> {
        /** the extra toMap call is because the asScala gives us a mutable map...  */
        val map = HashMap<String, org.quartz.Calendar>()
        val calendarCfg = config.getConfig("calendars").root()
        calendarCfg.forEach { name, value ->
            val childCfg = value.atKey(name)
            map[name] = parseCalendar(name, childCfg.getConfig(name), defaultTimezone)
        }
        return map
    }

    @Throws(ParseException::class)
    fun parseFmt(raw: String?, fmt: SimpleDateFormat, tz: TimeZone?): Calendar {
        val c = Calendar.getInstance(tz)
        c.time = fmt.parse(raw)
        return c
    }

    fun parseAnnualCalendar(name: String, config: Config, tz: TimeZone?): AnnualCalendar {
        val dateFmt = SimpleDateFormat("MM-dd")
        dateFmt.timeZone = tz
        val stringList = config.getStringList("excludeDates")
        if (stringList.isEmpty()) {
            throw IllegalArgumentException(
                "" +
                        "Invalid or Missing Configuration entry 'excludeDates' for Annual calendar " + name
                        + ". You must provide a list of ISO-8601 compliant dates ('YYYY-MM-DD')."
            )
        }
        val excludeDates = stringList.stream()
            .map { d: String ->
                try {
                    return@map parseFmt(d, dateFmt, tz)
                } catch (e: ParseException) {
                    throw IllegalArgumentException(
                        ("Invalid date " + d
                                + " in Annual Calendar " + name
                                + " - 'excludeDates'. You must provide an ISO-8601 compliant date ('YYYY-MM-DD').")
                    )
                }
            }.collect(Collectors.toList())
        val cal = AnnualCalendar()
        val list = ArrayList<Calendar>()
        list.addAll(excludeDates)
        cal.daysExcluded = list
        return cal
    }

    fun parseHolidayCalendar(name: String, config: Config, tz: TimeZone?): HolidayCalendar {
        val dateFmt = SimpleDateFormat("MM-dd")
        dateFmt.timeZone = tz
        val stringList = config.getStringList("excludeDates")
        if (stringList.isEmpty()) {
            throw IllegalArgumentException(
                ("" +
                        "Invalid or Missing Configuration entry 'excludeDates' for Annual calendar " + name
                        + ". You must provide a list of ISO-8601 compliant dates ('YYYY-MM-DD').")
            )
        }
        val excludeDates = stringList.stream()
            .map { d: String ->
                try {
                    return@map parseFmt(d, dateFmt, tz).getTime()
                } catch (e: ParseException) {
                    throw IllegalArgumentException(
                        ("Invalid date " + d
                                + " in Annual Calendar " + name
                                + " - 'excludeDates'. You must provide an ISO-8601 compliant date ('YYYY-MM-DD').")
                    )
                }
            }.collect(Collectors.toList())
        val cal = HolidayCalendar()
        excludeDates.forEach(Consumer { excludedDate: Date? ->
            cal.addExcludedDate(
                excludedDate
            )
        })
        return cal
    }

    fun parseDailyCalendar(name: String, config: Config): DailyCalendar {
        val parseTimeEntry =
            Function { path: String? -> config.getString(path) }
        var startTime: String = ""
        var endTime: String = ""
        try {
            startTime = parseTimeEntry.apply("exclude.startTime")
            endTime = parseTimeEntry.apply("exclude.endTime")
        } catch (e: ConfigException.Missing) {
            throw IllegalArgumentException(
                ("Invalid or Missing Configuration entry for Daily Calendar "
                        + name + ". You must provide a time in the format 'HH:MM[:SS[:mmm]]'")
            )
        } catch (e: WrongType) {
            throw IllegalArgumentException(
                ("Invalid or Missing Configuration entry for Daily Calendar "
                        + name + ". You must provide a time in the format 'HH:MM[:SS[:mmm]]'")
            )
        }
        return DailyCalendar(startTime, endTime)
    }

    fun parseWeeklyCalendar(name: String, config: Config): WeeklyCalendar {
        val excludeDays: List<Int>
        try {
            excludeDays = config.getIntList("excludeDays")
        } catch (e: ConfigException.Missing) {
            throw IllegalArgumentException(
                ("Invalid or Missing Configuration entry 'excludeDays' for Weekly Calendar "
                        + name + ". You must provide a list of Integers between 1 and 7.")
            )
        } catch (e: WrongType) {
            throw IllegalArgumentException(
                ("Invalid or Missing Configuration entry 'excludeDays' for Weekly Calendar "
                        + name + ". You must provide a list of Integers between 1 and 7.")
            )
        }
        var excludeWeekends = true
        try {
            excludeWeekends = config.getBoolean("excludeWeekends")
        } catch (e: ConfigException.Missing) {
        }
        excludeDays.forEach(Consumer { d: Int ->
            if (d < Calendar.SUNDAY || d > Calendar.SATURDAY) {
                throw IllegalArgumentException(
                    ("Weekly Calendar  " + name +
                            " - 'excludeDays' must consist of a list of Integers between 1 and 7")
                )
            }
        })
        val cal = WeeklyCalendar()
        excludeDays.forEach(Consumer { d: Int? ->
            cal.setDayExcluded(
                (d)!!,
                true
            )
        })
        if (!excludeWeekends) {
            if (excludeDays.contains(7) || excludeDays.contains(1)) throw IllegalArgumentException(
                ("Weekly Calendar " + name +
                        " - Cannot set 'excludeWeekends' to false when you have explicitly excluded Saturday (7) or Sunday (1)")
            ) else {
                cal.setDayExcluded(1, false)
                cal.setDayExcluded(7, false)
            }
        }
        return cal
    }

    fun parseMonthlyCalendar(name: String, config: Config): MonthlyCalendar {
        val excludeDays: List<Int>
        try {
            excludeDays = config.getIntList("excludeDays")
        } catch (e: ConfigException.Missing) {
            throw IllegalArgumentException(
                ("Invalid or Missing Configuration entry 'excludeDays' for Weekly Calendar "
                        + name + ". You must provide a list of Integers between 1 and 7.")
            )
        } catch (e: WrongType) {
            throw IllegalArgumentException(
                ("Invalid or Missing Configuration entry 'excludeDays' for Weekly Calendar "
                        + name + ". You must provide a list of Integers between 1 and 7.")
            )
        }
        excludeDays.forEach(Consumer { d: Int ->
            if (d < 1 || d > 31) {
                throw IllegalArgumentException(
                    ("Weekly Calendar  " + name +
                            " - 'excludeDays' must consist of a list of Integers between 1 and 31")
                )
            }
        })
        val cal = MonthlyCalendar()
        excludeDays.forEach(Consumer { d: Int? ->
            cal.setDayExcluded(
                (d)!!,
                true
            )
        })
        return cal
    }

    fun parseCronCalendar(name: String, config: Config): CronCalendar {
        val exclude: String
        val cal: CronCalendar
        try {
            exclude = config.getString("excludeExpression")
            CronExpression.validateExpression(exclude)
            cal = CronCalendar(exclude)
        } catch (e: ConfigException.Missing) {
            throw IllegalArgumentException(
                ("Invalid or Missing Configuration entry 'excludeExpression' for Cron Calendar "
                        + name + ". You must provide a valid Quartz CronExpression.")
            )
        } catch (e: WrongType) {
            throw IllegalArgumentException(
                ("Invalid or Missing Configuration entry 'excludeExpression' for Cron Calendar "
                        + name + ". You must provide a valid Quartz CronExpression.")
            )
        } catch (e: ParseException) {
            throw IllegalArgumentException(
                ("Invalid 'excludeExpression' for Cron Calendar " + name +
                        ". Failed to validate CronExpression.")
            )
        }
        return cal
    }

    fun parseCalendar(name: String, config: Config, defaultTimezone: TimeZone?): org.quartz.Calendar {
        // parse common attributes
        var timezone: TimeZone?
        var description: String? = ""
        try {
            // todo - this is bad, as Java silently swaps the timezone if it doesn't match...
            timezone = TimeZone.getTimeZone(config.getString("timezone"))
            description = config.getString("description")
        } catch (e: ConfigException.Missing) {
            timezone = defaultTimezone
        }
        val typ: String
        try {
            // todo - make this whole thing a pattern extractor?
            typ = config.getString("type")
        } catch (e: ConfigException.Missing) {
            throw IllegalArgumentException("Calendar Type must be defined for $name")
        }
        val cal: BaseCalendar
        when (typ.uppercase(Locale.getDefault())) {
            "ANNUAL" -> cal = parseAnnualCalendar(name, config, timezone)
            "HOLIDAY" -> cal = parseHolidayCalendar(name, config, timezone)
            "DAILY" -> cal = parseDailyCalendar(name, config)
            "MONTHLY" -> cal = parseMonthlyCalendar(name, config)
            "WEEKLY" -> cal = parseWeeklyCalendar(name, config)
            "CRON" -> cal = parseCronCalendar(name, config)
            else -> throw IllegalArgumentException(
                ("Unknown Quartz Calendar type" +
                        " " + typ + " for calendar " + name + ". " +
                        "Valid types are Annual, Holiday, Daily, Monthly, Weekly, and Cron.")
            )
        }
        cal.description = description
        cal.timeZone = timezone
        return cal
    }
}