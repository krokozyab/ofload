plugins {
    kotlin("jvm") version "2.2.20"
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
    testImplementation(kotlin("test"))
}

application {
    mainClass.set("app.UCPDataSource")
}

tasks.test {
    useJUnitPlatform()
}