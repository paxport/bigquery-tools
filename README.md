Big Query Tools
===================

Tooling for working with Google Big Query

Uses Spring.


* Use BigQueryTable to define columns and easily create the table and then insert data
* Use ReflectionBigQueryTable to automatically map a Java Class into a BigQueryTable
* Define a Query against BigQuery and run a QueryJob with BigQueryJobFactory


## To Release new version to Bintray

    mvn clean release:prepare -Darguments="-Dmaven.javadoc.skip=true"
    mvn release:perform -Darguments="-Dmaven.javadoc.skip=true"


