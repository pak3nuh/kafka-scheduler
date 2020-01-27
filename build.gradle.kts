plugins {
    `java-library`
}

group = "pt.pak3nuh.messaging.kafka"

allprojects {
    version = "0.3.0-SNAPSHOT"
}

subprojects {
    apply(plugin = "java-library")

    group = "pt.pak3nuh.messaging.kafka.scheduler"

    repositories {
        mavenLocal()
        mavenCentral()
    }

    dependencies {
        testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.0")
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
        sourceCompatibility = "1.8"
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}
