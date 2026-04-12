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
    implementation(files("libs/orfujdbc-1.0-SNAPSHOT.jar"))
    testImplementation(kotlin("test"))
}

application {
    mainClass.set("app.MainKt")
}

tasks.test {
    useJUnitPlatform()
}
