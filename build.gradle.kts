import com.jfrog.bintray.gradle.BintrayExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.util.*

plugins {
    kotlin("jvm") version "1.4.0"
    kotlin("kapt") version "1.4.0"
    id("io.spring.dependency-management") version "1.0.9.RELEASE"
    id("com.jfrog.bintray") version "1.8.5"
    `maven-publish`
}

(System.getProperty("release_version") ?: findProperty("release_version"))?.let { version = it.toString() }

repositories {
    mavenLocal()
    jcenter()
}

dependencyManagement {
    imports {
        mavenBom("io.vertx:vertx-dependencies:${libVersion("vertx")}")
        mavenBom("org.junit:junit-bom:${libVersion("junit")}")
        mavenBom("org.testcontainers:testcontainers-bom:${libVersion("testcontainers")}")
        mavenBom("software.amazon.awssdk:bom:${libVersion("awssdk")}")
    }
    generatedPomCustomization {
        setEnabled(false)
    }
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))
    api(vertx("vertx-core"))
    api(vertx("vertx-lang-kotlin"))
    api(vertx("vertx-lang-kotlin-coroutines"))
    api(vertx("vertx-service-discovery"))
    api(vertx("vertx-service-proxy"))
    compileOnly(vertx("vertx-codegen"))

    kapt("io.vertx:vertx-codegen:${libVersion("vertx")}:processor")

    api("software.amazon.awssdk:cloudwatch-metric-publisher:${libVersion("awssdk")}")
    api(awsSdk("kinesis"))
    api(awsSdk("dynamodb"))
    api(awsSdk("sts"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:${libVersion("coroutines")}")
    implementation("ch.sourcemotion.vertx.redis:vertx-redis-client-heimdall:${libVersion("vertx-redis-heimdall")}")

    api("io.reactiverse:aws-sdk:${libVersion("vertx-aws-sdk")}") {
        exclude(group = "software.amazon.awssdk", module = "*")
    }

    api("com.fasterxml.jackson.module:jackson-module-kotlin:${dependencyManagement.importedProperties["jackson.version"]}")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${dependencyManagement.importedProperties["jackson.version"]}")
    api("io.github.microutils:kotlin-logging:${libVersion("kotlin-logging")}")

    testImplementation(kotlin("test-junit"))
    testImplementation("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("org.junit.vintage:junit-vintage-engine")
    testImplementation("io.kotest:kotest-assertions-core-jvm:${libVersion("kotest")}")
    testImplementation(vertx("vertx-junit5"))
    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:${libVersion("mockito-kotlin")}")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:${libVersion("log4j")}")
    testImplementation("org.apache.logging.log4j:log4j-core:${libVersion("log4j")}")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:localstack")
    testImplementation("org.testcontainers:toxiproxy")
    testImplementation("com.amazonaws:aws-java-sdk-core:${libVersion("awssdk-old")}") {
        exclude("io.netty", "*")
    }
}

fun vertx(module: String) = "io.vertx:$module:${libVersion("vertx")}"
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
        environment(Pair("AWS_CBOR_DISABLE", "true"), Pair("CBOR_ENABLED", "false"), Pair("aws.cborEnabled", "false"))
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
            name = project.version.toString()
            released = Date().toString()
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
                version = project.version.toString()
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
