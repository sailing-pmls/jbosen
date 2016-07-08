# Building JBosen

To get started, please download the JBösen source code using
```
git clone https://github.com/petuum/jbosen.git
cd jbosen
```

## Java
JBösen requires Java version 7 or above. Please make sure that the JDK is installed and the `$JAVA_HOME` environment variable is set to the correct path.

## Gradle
JBösen uses [Gradle](http://gradle.org) as its dependency manager. Gradle is not required to build JBösen, since you can use the script `gradlew` in the top-level directory of the repository.

## Building JBösen

To build all of the source files included, simply run
```
./gradlew build
```
This will build JBösen, the built-in applications in `app/`, as well as modules for YARN support.

## Hadoop/YARN (Only for running on YARN cluster)
1. Install and configure HDFS and YARN on the cluster properly.
2. Find out the version of HDFS and YARN, and then change the variable `yarnVersion` in `petuum-java/build.gradle` to the version number of HDFS and YARN on the cluster. The default value is `2.6.0`.

Detailed instructions on running on YARN can be found [[here|Launch using YARN]].

<sub> Note: The python running scripts we prepared requires python 2.7. </sub>
