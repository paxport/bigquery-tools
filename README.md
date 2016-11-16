Big Query Tools
===================

Tooling for working with Google Big Query

Uses Spring.


* Use BigQueryTable to define columns and easily create the table and then insert data
* Use ReflectionBigQueryTable to automatically map a Java Class into a BigQueryTable
* Define a Query against BigQuery and run a QueryJob with BigQueryJobFactory

## JCenter Dependency

Add JCenter to your repositories if not already:

    <repositories>
        <repository>
            <id>jcenter-snapshots</id>
            <name>jcenter</name>
            <url>https://jcenter.bintray.com/</url>
        </repository>
    </repositories>
    
Add cloud audit dependency:

    <dependency>
        <groupId>com.paxport</groupId>
        <artifactId>bigquery-tools</artifactId>
        <version>1.2.1</version>
    </dependency>


## To Release new version to Bintray

    mvn clean release:prepare -Darguments="-Dmaven.javadoc.skip=true"
    mvn release:perform -Darguments="-Dmaven.javadoc.skip=true"


