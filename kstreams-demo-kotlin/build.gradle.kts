import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
  id("org.springframework.boot") 
  id("io.spring.dependency-management")
  kotlin("jvm") version "1.3.72"
  kotlin("plugin.spring") version "1.3.72"
}

group = "io.developer.confluent"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_11

repositories {
  mavenCentral()
}

dependencies {
  implementation("org.springframework.boot:spring-boot-starter")
  implementation("org.apache.kafka:kafka-streams")
  implementation("org.jetbrains.kotlin:kotlin-reflect")
  implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
  implementation("org.springframework.kafka:spring-kafka")
  implementation("com.fasterxml.jackson.module:jackson-module-kotlin")

  testImplementation("org.springframework.boot:spring-boot-starter-test") {
    exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
  }
  testImplementation("org.springframework.kafka:spring-kafka-test")
  testImplementation("org.apache.kafka:kafka-streams-test-utils")
  //testImplementation("junit:junit:4.13")
}

tasks.withType<Test> {
  useJUnitPlatform()
  // Show test results.
  testLogging {
    events ("passed", "skipped", "failed")
  }
}

tasks.withType<KotlinCompile> {
  kotlinOptions {
    freeCompilerArgs = listOf("-Xjsr305=strict")
    jvmTarget = "11"
  }
}
