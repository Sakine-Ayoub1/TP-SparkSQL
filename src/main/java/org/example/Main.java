package org.example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
                .appName("TP SPARK SQL").master("local[*]")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().option("header","true").option("inferSchema","true").csv("incidents.csv");
        df.printSchema();
        df.show();
        df.groupBy("service").count().show();
        df.createTempView("incidents");
        sparkSession.sql("select EXTRACT(YEAR FROM to_date(date, 'dd/MM/yyyy')) AS annee,COUNT(*) AS nombre_incidents from incidents GROUP BY EXTRACT(YEAR FROM to_date(date, 'dd/MM/yyyy')) ORDER BY nombre_incidents DESC LIMIT 2").show();
    }
}