import org.apache.spark.sql.SparkSession;


public class SparkQueryJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Spark Test Job")
                .master("localhost:8000")
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("DROP DATABASE IF EXISTS spark_tests");
    }
}
