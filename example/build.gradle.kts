import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    kotlin("jvm") version "1.3.72"
}

repositories {
    mavenLocal()
    jcenter()
}

val orchestraVersion = "0.5.0"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("ch.sourcemotion.vertx:vertx-kinesis-consumer-orchestra:$orchestraVersion")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${findProperty("version.log4j")}")
    implementation("org.apache.logging.log4j:log4j-core:${findProperty("version.log4j")}")
}

tasks {
    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}
