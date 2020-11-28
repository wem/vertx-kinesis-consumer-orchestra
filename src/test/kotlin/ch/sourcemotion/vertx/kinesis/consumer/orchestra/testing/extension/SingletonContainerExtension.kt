package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.extension

import mu.KLogging
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.platform.commons.util.AnnotationUtils
import org.junit.platform.commons.util.ReflectionUtils
import org.testcontainers.containers.GenericContainer

class SingletonContainerExtension : BeforeAllCallback {

    private companion object : KLogging()

    override fun beforeAll(context: ExtensionContext) {
        val containers = findContainers(context)
        containers.forEach { container ->
            container.start()
            logger.info { "---- ${container.dockerImageName} started ----" }
        }
    }

    private fun findContainers(context: ExtensionContext): List<GenericContainer<*>> {
        val testClass = context.testClass
        return if (testClass.isPresent) {
            ReflectionUtils.findMethods(
                testClass.get(),
                {
                    AnnotationUtils.isAnnotated(it,
                        SingletonContainer::class.java
                    ) && ReflectionUtils.isStatic(it)
                },
                ReflectionUtils.HierarchyTraversalMode.TOP_DOWN
            ).map { it.invoke(null) }.filterIsInstance<GenericContainer<*>>()
        } else {
            emptyList()
        }
    }
}


@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.PROPERTY_GETTER)
annotation class SingletonContainer
