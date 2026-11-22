plugins {
	`java-library`
	kotlin("jvm") version "2.4.21"
	kotlin("plugin.serialization") version "2.4.21"
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
	api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.12.0")
}
