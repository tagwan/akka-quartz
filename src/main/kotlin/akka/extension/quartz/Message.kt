package akka.extension.quartz

import java.io.Serializable
import java.util.*

class Message {
    // region RequireFireTime
    class RequireFireTime(val msg: Any) : Serializable {

        override fun equals(obj: Any?): Boolean {
            if (obj !is RequireFireTime) {
                return false
            }
            return msg === obj.msg
        }

        override fun toString(): String {
            return "Message.RequireFireTime($msg)"
        }
    }

    // endregion
    class WithFireTime(val msg: Any, val scheduledFireTime: Date) : Serializable {
        var previousFiringTime: Date? = null
            private set
        var nextFiringTime: Date? = null
            private set

        constructor(msg: Any, scheduledFireTime: Date, previousFiringTime: Date?, nextFiringTime: Date?) : this(
            msg,
            scheduledFireTime
        ) {
            this.previousFiringTime = previousFiringTime
            this.nextFiringTime = nextFiringTime
        }
    }
}