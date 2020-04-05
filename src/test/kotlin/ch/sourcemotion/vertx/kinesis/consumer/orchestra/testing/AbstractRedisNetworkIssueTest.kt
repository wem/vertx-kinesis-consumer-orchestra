package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing


abstract class AbstractRedisNetworkIssueTest : AbstractRedisTest() {

//    @JvmField
//    @Container
//    var toxiproxy: ToxiproxyContainer = ToxiproxyContainer("shopify/toxiproxy:2.1.4")
//        .withNetwork(network)

//    private lateinit var proxyContainer: ToxiproxyContainer.ContainerProxy
//    private lateinit var toxics : ToxicList
//
//    @BeforeEach
//    internal fun setUpAbstractRedisNetworkIssueTest() {
//        proxyContainer = toxiproxy.getProxy(redisContainer, 6370)
//        toxics = proxyContainer.toxics()
//    }
//
//    protected suspend fun cutConnection(durationMillis: Long) {
//        suspendCancellableCoroutine<Unit> {cont ->
//            GlobalScope.launch(Dispatchers.IO) {
//                proxyContainer.setConnectionCut(true)
//                cont.resume(Unit)
//                delay(durationMillis)
//                proxyContainer.setConnectionCut(false)
//            }
//        }
//    }
//
//    protected fun getToxProxyAddress(): String = proxyContainer.containerIpAddress
//    protected fun getToxiProxyPort() : Int = proxyContainer.proxyPort
}
