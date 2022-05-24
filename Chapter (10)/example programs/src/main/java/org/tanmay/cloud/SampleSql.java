package org.tanmay.cloud;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SampleSql {
    public static void main (String[] args) {
        if (args.length < 1) {
            //Sample data stored at ../../../resources/example-data/people.json
            //You need to provide absolute path to the data file
            System.err.println("Usage : SampleSql <json path name>");
            System.exit(1);
        }
        String jsonPath = args[0];
        SparkSession session = SparkSession.builder().appName("Sample SQL App").getOrCreate();
        Dataset<Row> dataFrame = session.read().json(args[0]);
        //Print the data in a tabular format on stdout
        dataFrame.show();
        //Show all rows, with age incremented by 5, demonstrates untyped trnasformation
        dataFrame.select(dataFrame.col("name"), dataFrame.col("age").plus(5)).show();
        //Show schema of the dataFrame
        dataFrame.printSchema();
        //Show only a single column
        dataFrame.select(dataFrame.col("name")).show();
        //Print only a single row
        dataFrame.select(dataFrame.col("age"), dataFrame.col("name")).show(1);
        //Select people older than 29
        dataFrame.filter(dataFrame.col("age").gt(29)).show();
        //Group by age
        dataFrame.groupBy(dataFrame.col("age")).count().show();

        //Create a temporary view(table) from the dataframe
        dataFrame.registerTempTable("people");
        //Run a SQL query directly and accept result in a dateframe
        Dataset<Row> dataFrame1 = session.sql("select people.name as NAME, people.age as AGE from people");
        dataFrame1.show();
    }
}
