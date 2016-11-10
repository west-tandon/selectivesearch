![Diagram](http://msiedlaczek.com/phd/selectivesearch/diagram.png)

# Compilation & Running

To compile this project, you will need [**sbt**](http://www.scala-sbt.org) installed.

## Executable

To create an executable package with all dependencies, run:

```bash
sbt stage
```

After successful run, the package will be created in `target/universal/stage`.

To run the application, execute `bin/selectivesearch` (or `bin/selectivesearch.bat` on Windows), e.g.:

```bash
./bin/selectivesearch bucketize [basename]
```

To see available commands, read [documentation](https://github.com/elshize/selectivesearch/wiki/Transformations).

## Dependency

You can use this project as dependency by publishing it to a local Ivy or Maven repository:

```bash
sbt publish-local # Ivy
sbt publishM2 # Maven
```

Then, you can import to your project using sbt:

```scala
libraryDependencies += "edu.nyu.tandon" %% "selectivesearch" % "1.0"
```

or Maven:

```xml
<dependency>
    <groupId>edu.nyu.tandon</groupId>
    <artifactId>selectivesearch</artifactId>
    <version>1.0</version>
</dependency>
```
