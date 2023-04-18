import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SparkTestJob {
    public static String warehouseLocation = new File("/tmp/spark-warehouse").getAbsolutePath();

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Test Job")
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("CREATE DATABASE IF NOT EXISTS spark_tests");
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS spark_tests.s3_table_1 (id INT, element STRING) STORED AS PARQUET LOCATION 's3a://spark/s3_table_1'");

        List<Tuple2<Integer, String>> list = new ArrayList<>();
        for (int i = 1; i <= 30; i++) {
            list.add(new Tuple2<>(i, "elem_" + i));
        }

        Dataset<Row> df = spark.createDataset(list, Encoders.tuple(Encoders.INT(), Encoders.STRING()))
                .toDF("id", "element");
        df.write()
                .mode(SaveMode.Overwrite)
                .option("path", "s3a://spark/s3_table_1")
                .saveAsTable("spark_tests.s3_table_1");

        long r = spark.sql("SELECT * FROM spark_tests.s3_table_1").count();
        System.out.println(r);
    }
}
