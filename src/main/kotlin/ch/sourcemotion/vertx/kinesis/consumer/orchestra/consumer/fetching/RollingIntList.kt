package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

internal class RollingIntList(private val capacity: Int) : ArrayList<Int>() {
    override fun add(element: Int) = super.add(element).also {
        val exceeded = size - capacity
        if (exceeded > 0) {
            removeRange(0, exceeded)
        }
    }

    val full: Boolean
        get() = size == capacity
}
