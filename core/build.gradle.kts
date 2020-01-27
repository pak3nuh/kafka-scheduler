plugins {
    id("io.freefair.lombok") version "4.1.6"
}

dependencies {
    api(project(":api"))
    implementation("org.apache.kafka:kafka-clients:2.4.0")
    testImplementation("org.mockito:mockito-core:3.2.4")
}

