# Scala Spark Project with Extract and Transform

This Spark project demonstrates data extraction and transformation using two main files: `Extract.scala` and `Transform.scala`. The project is managed using SBT (Simple Build Tool) for easy setup and execution.

## Prerequisites

Before you begin, ensure you have the following installed on your system:

- Java Development Kit (JDK)
- Scala
- SBT (Simple Build Tool)
- Apache Spark

## Running the project 
- Create a folder where the project will be excuted
- run the following command in the termnal for dependencies "sbt package"
- then run the follwoing command for data extraction, this will create a teams-season folder and jon files for each team:
 "spark-submit   --class Extract   --master local[*]   target/scala-2.12/nba_2.12-1.0.jar"

- After that run the followng command for extraction from the json files and process the data the result will be a directory where the csv file is stored:
"spark-submit   --class Transform   --master local[*]   target/scala-2.12/nba_2.12-1.0.jar"
