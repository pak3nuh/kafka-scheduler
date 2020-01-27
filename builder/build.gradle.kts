plugins {
    id("io.freefair.lombok") version "4.1.6"
}

dependencies {
    implementation(project(":core"))
    implementation("org.apache.kafka:kafka-clients:2.4.0")
    api(project(":api"))
}

