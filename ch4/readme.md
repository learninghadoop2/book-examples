This is a skeleton layout for development of a Samza task. To customize:

1. Set the project name and version in gradle.properties
2. Create task config files in src/main/resources

To build and deploy a task:
1. ./gradlew targz (depends on build so does full compile cycle)
2. ./gradlew run<task name> -- look at tasks in build.gradle for examples
