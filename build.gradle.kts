import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.owasp.dependencycheck.gradle.extension.AnalyzerExtension

plugins {
    kotlin("jvm") version "1.6.10"
    kotlin("kapt") version "1.6.10"
    id("org.jetbrains.dokka") version "1.6.10"
    id("org.owasp.dependencycheck") version "6.5.2.1"
    `maven-publish`
    signing
}

repositories {
    mavenCentral()
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
    api("ch.sourcemotion.vertx:vertx-redis-client-heimdall:${libVersion("vertx-redis-heimdall")}")

    api("io.reactiverse:aws-sdk:${libVersion("vertx-aws-sdk")}", AwsSdkExclusion)

    api("com.fasterxml.jackson.module:jackson-module-kotlin:${libVersion("jackson")}")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${libVersion("jackson")}")
    api("com.fasterxml.jackson.dataformat:jackson-dataformat-xml") { version { strictly("${libVersion("jackson")}") } }
    api("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor") { version { strictly("${libVersion("jackson")}") } }
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
    testImplementation(awsSdk("sso"), JacksonExclusion)
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

dependencyCheck {
    failBuildOnCVSS = 7.0F // High and higher
    autoUpdate = true
    analyzers(closureOf<AnalyzerExtension> {
        assemblyEnabled = false
    })
    skipConfigurations.addAll(configurations.filter { it.name.contains("dokka") }.map { it.name })
}

tasks {
    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "11"
        kotlinOptions.freeCompilerArgs += listOf("-Xuse-experimental=kotlin.contracts.ExperimentalContracts", "-Xinline-classes")
    }
    withType<Test> {
        maxParallelForks = 1
        useJUnitPlatform()
        systemProperties["vertx.logger-delegate-factory-class-name"] = "io.vertx.core.logging.SLF4JLogDelegateFactory"
        environment("AWS_REGION" to findProperty("AWS_REGION"), "AWS_PROFILE" to findProperty("AWS_PROFILE"), "REDIS_VERSION" to "5")
    }

    val redis6Test by registering(Test::class) {
        maxParallelForks = 1
        useJUnitPlatform()
        systemProperties["vertx.logger-delegate-factory-class-name"] = "io.vertx.core.logging.SLF4JLogDelegateFactory"
        environment("AWS_REGION" to findProperty("AWS_REGION"), "AWS_PROFILE" to findProperty("AWS_PROFILE"), "REDIS_VERSION" to "6")
    }

    build.get().dependsOn.add(redis6Test)
}

val publicationName = "VKCO"

val publishUsername: String by lazy {
    "${findProperty("ossrhUsername")}"
}
val publishPassword: String by lazy {
    "${findProperty("ossrhPassword")}"
}

val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

val javadocJar by tasks.registering(Jar::class) {
    dependsOn.add(tasks.dokkaJavadoc)
    archiveClassifier.set("javadoc")
    from("$buildDir/dokka/javadoc")
}

val publishUrl = if ("$version".endsWith("SNAPSHOT")) {
    "https://s01.oss.sonatype.org/content/repositories/snapshots/"
} else {
    "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/"
}

publishing {
    publications {
        repositories {
            maven {
                name = "ossrh"
                setUrl(publishUrl)
                credentials {
                    username = publishUsername
                    password = publishPassword
                }
            }
        }

        create(publicationName, MavenPublication::class.java) {
            from(components["java"])
            artifact(sourcesJar.get())
            artifact(javadocJar.get())

            pom {
                groupId = groupId
                artifactId = artifactId
                version = "${project.version}"
                packaging = "jar"
                name.set("Vert.x Kinesis consumer orchestra")
                description.set("Reactive alternative to KCL, based on Vert.x and implemented in Kotlin")
                url.set("https://github.com/wem/vertx-kinesis-consumer-orchestra")
                scm {
                    connection.set("scm:https://github.com/wem/vertx-kinesis-consumer-orchestra.git")
                    developerConnection.set("scm:https://github.com/wem/vertx-kinesis-consumer-orchestra.git")
                    url.set("https://github.com/wem/vertx-kinesis-consumer-orchestra")
                }
                licenses {
                    license {
                        name.set("The MIT License")
                        url.set("https://www.opensource.org/licenses/MIT")
                        distribution.set("https://github.com/wem/vertx-kinesis-consumer-orchestra")
                    }
                }
                developers {
                    developer {
                        id.set("Michel Werren")
                        name.set("Michel Werren")
                        email.set("michel.werren@source-motion.ch")
                    }
                }
            }
        }
    }
}

signing {
    sign(publishing.publications[publicationName])
}
