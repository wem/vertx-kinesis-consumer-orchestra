import com.jfrog.bintray.gradle.BintrayExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.util.*


plugins {
    kotlin("jvm") version "1.3.71"
    id("io.spring.dependency-management") version "1.0.9.RELEASE"
    id("com.jfrog.bintray") version "1.8.5"
    `maven-publish`
}

repositories {
    mavenLocal()
    jcenter()
}

dependencyManagement {
    imports {
        mavenBom("io.vertx:vertx-dependencies:${version("vertx")}")
        mavenBom("software.amazon.awssdk:bom:${version("awssdk")}")
        mavenBom("org.junit:junit-bom:${version("junit")}")
        mavenBom("org.testcontainers:testcontainers-bom:${version("testcontainers")}")
    }
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(vertx("vertx-core"))
    implementation(vertx("vertx-redis-client"))
    implementation(vertx("vertx-lang-kotlin"))
    implementation(vertx("vertx-lang-kotlin-coroutines"))
    implementation(vertx("vertx-service-discovery"))

    implementation("software.amazon.awssdk:kinesis")
    implementation("software.amazon.awssdk:netty-nio-client")
    implementation("software.amazon.awssdk:sts")
    implementation("io.reactiverse:vertx-aws-sdk:${version("vertx-aws-sdk")}")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:${dependencyManagement.importedProperties["jackson.version"]}")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${dependencyManagement.importedProperties["jackson.version"]}")
    implementation("io.github.microutils:kotlin-logging:${version("kotlintest-logging")}")

    testImplementation(kotlin("test-junit"))
    testImplementation("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("org.junit.vintage:junit-vintage-engine")
    testImplementation("io.kotlintest:kotlintest-runner-junit5:${version("kotlintest-runner-junit5")}")
    testImplementation(vertx("vertx-junit5"))
    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:${version("mockito-kotlin")}")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:${version("log4j")}")
    testImplementation("org.apache.logging.log4j:log4j-core:${version("log4j")}")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:localstack")
    testImplementation("com.amazonaws:aws-java-sdk-core:${version("awssdk-old")}")
}

fun vertx(module: String) = "io.vertx:$module"
fun version(suffix: String) = property("version.$suffix")

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

System.getProperty("version")?.let { version = it }

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
        vcsUrl = "https://gitlab.com/michel.werren/vertx-retry"
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
