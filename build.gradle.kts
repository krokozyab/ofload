plugins {
    kotlin("jvm") version "2.2.20"
    kotlin("plugin.serialization") version "2.2.20"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.oracle.database.jdbc:ojdbc11:23.4.0.24.05")
    implementation("com.oracle.database.jdbc:ucp11:23.4.0.24.05")
    implementation("com.oracle.database.security:oraclepki:23.4.0.24.05")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("ch.qos.logback:logback-classic:1.4.14")
    implementation("net.logstash.logback:logstash-logback-encoder:7.4")
    implementation(files("libs/orfujdbc-1.0-SNAPSHOT.jar"))
    testImplementation(kotlin("test"))
}

application {
    mainClass.set("app.MainKt")
}

tasks.test {
    useJUnitPlatform()
}
