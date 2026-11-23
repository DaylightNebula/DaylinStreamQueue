plugins {
	application
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
	implementation("com.daylin:daylin-sq-client-java:0.0.1-SNAPSHOT")
}

application {
	mainClass = "io.daylightnebula.streamqueue.tests.producer.ProducerKt"
}
