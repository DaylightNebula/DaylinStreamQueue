plugins {
	`java-library`
	kotlin("jvm") version "2.4.21"
}

group = "com.daylin"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(25)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	api("com.daylin:daylin-sq-core:0.0.1-SNAPSHOT")
	testImplementation(kotlin("test-junit5"))
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
