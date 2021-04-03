import com.jfrog.bintray.gradle.BintrayExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.util.*

plugins {
    kotlin("jvm") version "1.4.32"
    kotlin("kapt") version "1.4.32"
    id("com.jfrog.bintray") version "1.8.5"
    `maven-publish`
}

repositories {
    mavenLocal()
    jcenter()
}

dependencies {
    implementation(platform("io.vertx:vertx-dependencies:${libVersion("vertx")}"))
    implementation(platform("org.junit:junit-bom:${libVersion("junit")}"))
    implementation(platform("org.testcontainers:testcontainers-bom:${libVersion("testcontainers")}"))
    implementation(platform("software.amazon.awssdk:bom:${libVersion("awssdk")}"))

    api(kotlin("stdlib-jdk8"))
    api(kotlin("reflect"))
    api(vertx("vertx-core"))
    api(vertx("vertx-lang-kotlin"))
    api(vertx("vertx-lang-kotlin-coroutines"))
    api(vertx("vertx-service-discovery"))
    api(vertx("vertx-service-proxy"))
    api(vertx("vertx-micrometer-metrics"))
    compileOnly(vertx("vertx-codegen"))

    kapt("io.vertx:vertx-codegen:${libVersion("vertx")}:processor")

    api("software.amazon.awssdk:cloudwatch-metric-publisher:${libVersion("awssdk")}", JacksonExclusion)
    api(awsSdk("kinesis"), JacksonExclusion)
    api(awsSdk("netty-nio-client"), JacksonExclusion)
    api(awsSdk("dynamodb"), JacksonExclusion)
    api(awsSdk("sts"), JacksonExclusion)
    api("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:${libVersion("coroutines")}")
    api("ch.sourcemotion.vertx.redis:vertx-redis-client-heimdall:${libVersion("vertx-redis-heimdall")}")

    api("io.reactiverse:aws-sdk:${libVersion("vertx-aws-sdk")}", AwsSdkExclusion)

    api("com.fasterxml.jackson.module:jackson-module-kotlin:${libVersion("jackson")}")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${libVersion("jackson")}")
    api("io.github.microutils:kotlin-logging:${libVersion("kotlin-logging")}")

    testImplementation(kotlin("test-junit"))
    testImplementation("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("org.junit.vintage:junit-vintage-engine")
    testImplementation("io.kotest:kotest-assertions-core-jvm:${libVersion("kotest")}")
    testImplementation(vertx("vertx-junit5"))
    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:${libVersion("mockito-kotlin")}")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:${libVersion("log4j")}")
    testImplementation("org.apache.logging.log4j:log4j-core:${libVersion("log4j")}")
    testImplementation("org.testcontainers:localstack", JacksonExclusion)
    testImplementation("org.testcontainers:toxiproxy", JacksonExclusion)
    testImplementation("com.amazonaws:aws-java-sdk-core:${libVersion("awssdk-old")}", NettyExclusion)
}

object AwsSdkExclusion : DependencyExclusion(
    mapOf(
        "software.amazon.awssdk" to listOf("*")
    )
)

object NettyExclusion : DependencyExclusion(
    mapOf(
        "io.netty" to listOf("*")
    )
)

object JacksonExclusion : DependencyExclusion(
    mapOf(
        "com.fasterxml.jackson.core" to listOf("*"),
        "com.fasterxml.jackson.dataformat" to listOf("*"),
        "com.fasterxml.jackson.datatype" to listOf("*"),
        "com.fasterxml.jackson.module" to listOf("*")
    )
)

abstract class DependencyExclusion(private val dependencies: Map<String, List<String>>) :
    Action<ExternalModuleDependency> {
    override fun execute(t: ExternalModuleDependency) {
        dependencies.forEach {
            val group = it.key
            val modules = it.value
            modules.forEach { module ->
                t.exclude(group, module)
            }
        }
    }
}

fun vertx(module: String) = "io.vertx:$module"
fun awsSdk(module: String) = "software.amazon.awssdk:$module"
fun libVersion(suffix: String) = property("version.$suffix")

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

tasks {
    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
        kotlinOptions.freeCompilerArgs = kotlinOptions.freeCompilerArgs.toMutableList().apply {
            add("-Xuse-experimental=kotlin.contracts.ExperimentalContracts")
            add("-Xinline-classes")
        }
    }
    withType<Test> {
        useJUnitPlatform()
        systemProperties["vertx.logger-delegate-factory-class-name"] = "io.vertx.core.logging.SLF4JLogDelegateFactory"
        environment("AWS_CBOR_DISABLE" to "true", "CBOR_ENABLED" to "false", "aws.cborEnabled" to "false",
            "DEFAULT_REGION" to "us-east-1")
    }
}

val groupId = "ch.sourcemotion.vertx"
val artifactId = "vertx-kinesis-consumer-orchestra"
val publicationName = "vertxKinesisConsumerOrchestra"

val bintrayUser: String by lazy {
    "${findProperty("bintray_user")}"
}
val bintrayApiKey: String by lazy {
    "${findProperty("bintray_api_key")}"
}

bintray {
    user = bintrayUser
    key = bintrayApiKey
    setPublications(publicationName)

    pkg(closureOf<BintrayExtension.PackageConfig> {
        repo = "maven"
        name = artifactId
        userOrg = "michel-werren"
        vcsUrl = "https://github.com/wem/vertx-kinesis-consumer-orchestra"
        version(closureOf<BintrayExtension.VersionConfig> {
            name = "${project.version}"
            released = "${Date()}"
        })
        setLicenses("MIT")
    })
}

val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

publishing {
    publications {
        register(publicationName, MavenPublication::class.java) {
            from(components["java"])
            artifact(sourcesJar.get())
            pom {
                groupId = groupId
                artifactId = artifactId
                version = "${project.version}"
                licenses {
                    license {
                        name.set("The MIT License")
                        url.set("http://www.opensource.org/licenses/MIT")
                        distribution.set("https://github.com/wem/vertx-kinesis-consumer-orchestra")
                    }
                }
            }
        }
    }
}
