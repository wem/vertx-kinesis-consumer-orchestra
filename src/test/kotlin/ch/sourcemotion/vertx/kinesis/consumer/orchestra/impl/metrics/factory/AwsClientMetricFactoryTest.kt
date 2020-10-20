package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractVertxTest
import com.nhaarman.mockitokotlin2.mock
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.vertx.core.Vertx
import org.junit.jupiter.api.Test
import software.amazon.awssdk.metrics.MetricPublisher
import kotlin.test.assertFailsWith

class AwsClientMetricFactoryTest : AbstractVertxTest() {

    @Test
    internal fun successfully_create_metric_publisher() {
        val options = object : AwsClientMetricOptions {
            override var enabled: Boolean = true
            override var factoryClassName: String? = TestableAwsClientMetricFactory::class.qualifiedName
        }

        val metricPublisher = AwsClientMetricFactory.create(vertx, options)
        metricPublisher.shouldNotBeNull()
    }

    @Test
    internal fun disabled_options_leads_to_null_metric_publisher() {
        val metricPublisher = AwsClientMetricFactory.create(vertx, DisabledAwsClientMetricOptions())
        metricPublisher.shouldBeNull()
    }

    @Test
    internal fun class_not_found_with_unavailable_factory() {
        val options = object : AwsClientMetricOptions {
            override var enabled: Boolean = true
            override var factoryClassName: String? = "unavailable.factory.class"
        }
        assertFailsWith<ClassNotFoundException> {
            AwsClientMetricFactory.create(vertx, options)
        }
    }

    @Test
    internal fun illegal_argument_with_not_a_factory() {
        val options = object : AwsClientMetricOptions {
            override var enabled: Boolean = true
            override var factoryClassName: String? = AwsClientMetricFactoryTest::class.qualifiedName
        }
        assertFailsWith<IllegalArgumentException> {
            AwsClientMetricFactory.create(vertx, options)
        }
    }
}

class TestableAwsClientMetricFactory : AwsClientMetricFactory<AwsClientMetricOptions>() {
    private val metricPublisher = mock<MetricPublisher> {}

    override fun createMetricsPublisher(vertx: Vertx, options: AwsClientMetricOptions?): MetricPublisher {
        return metricPublisher
    }
}
