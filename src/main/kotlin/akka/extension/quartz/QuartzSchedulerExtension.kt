package akka.extension.quartz

import akka.actor.*
import akka.extension.quartz.impl.SimpleQuartzExtension

/**
 * QuartzSchedulerExtension
 *
 * @data 2022/5/27 17:54
 */
class QuartzSchedulerExtension : AbstractExtensionId<SimpleQuartzExtension?>(),
    ExtensionIdProvider {

    // The lookup method is required by ExtensionIdProvider,
    // so we return ourselves here, this allows us
    // to configure our extension to be loaded when
    // the ActorSystem starts up
    override fun lookup(): ExtensionId<out Extension?> {
        return quartzExtensionProvider
    }

    // This method will be called by Akka
    // to instantiate our Extension
    override fun createExtension(system: ExtendedActorSystem): SimpleQuartzExtension {
        return SimpleQuartzExtension(system)
    }

    override fun get(system: ActorSystem): SimpleQuartzExtension? {
        return super.get(system)
    }

    companion object {
        // This will be the identifier of our CountExtension
        val quartzExtensionProvider = QuartzSchedulerExtension()
    }
}