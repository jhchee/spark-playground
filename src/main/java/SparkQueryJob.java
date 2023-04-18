import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkQueryJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Spark Test Job")
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> df = spark.sql("SELECT * FROM spark_tests.s3_table_1");
        df.show();
    }
}
