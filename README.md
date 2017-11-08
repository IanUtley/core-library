# core-library 

The core-library tree is a collection of useful DataFlow Operators and miscellaneous examples.

##Operator list

* Start Node

     A node which has an optional input port intended to send input parameters to.

* Stop Node

     A node which has an optional output port intended to return a set of data.

* SubJob Executor Node

     A node which can call an exported DataFlow graph (a .dr file in JSON format), passing
     inputs into the workflows *Start Node* (if exists), and outputing data from the 
     workflows *Stop Node*.

* Sessionize Node

     A node which groups timestamped data into 'sessions' with custom time intervals.

* Derive Group Node

     A different UI on top of the existing 'Group' operator. This version allows the
     aggregations to entered as text expressions.

     Additionally, custom aggregates can now be used in KNIME, e.g. The 'concat' aggregator
     supplied as an example.

* Lead Lag Node

     An example node which re-presents timestamped data with lead/lag values on the same row.

* Zip Rows Node

     An example node which allows multiple independent streams of data to be joined without specifying any join conditions.

* Other functions

     * An example 'ROT13' implementation for use in *Derive Fields*.
     * An example 'UUID' implementation for use in *Derive Fields*.

## Configuration

Before building core-library you need to define the following variables to point to the local DataFlow update site [dataflow-p2-site](https://github.com/ActianCorp/dataflow-p2-site) root directory and the DataFlow version.
These should be defined in settings.xml as properties of an active profile.
e.g.
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <profiles>
    <profile>
      <id>default</id>
      <properties>
        <user.DATAFLOW_VER>6.6.0.111</user.DATAFLOW_VER>
        <user.DATAFLOW_REPO_HOME>${user.home}/dataflow-p2-site</user.DATAFLOW_REPO_HOME>
      </properties>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>default</activeProfile>
  </activeProfiles>
</settings>

## Building

The update site is built using [Apache Maven 3.0.5 or later](http://maven.apache.org/).

To build, run:

    mvn clean install
    
You can update the version number by running

    mvn org.eclipse.tycho:tycho-versions-plugin:set-version -DnewVersion=version
    
where version is of the form x.y.z or x.y.z-SNAPSHOT.
    

## Using the operators with the DataFlow Engine

The build generates a JAR file in the target directory under
[core-library/DataflowExtensions](https://github.com/ActianCorp/core-library/tree/master/DataflowExtensions)
with a name similar to 

    core-dataflow-1.y.z.jar

which can be included on the classpath when using the DataFlow engine.

## Installing the operators in KNIME

The build also produces a ZIP file which can be used as an archive file with the KNIME 'Help/Install New Software...' dialog.
The ZIP file can be found in the target directory under
[core-library/KnimeExtensions/Knime-Update](https://github.com/ActianCorp/core-library/tree/master/KnimeExtensions/Knime-Update) 
and with a name like 


    com.actian.services.knime.core.update-1.y.z.zip
 