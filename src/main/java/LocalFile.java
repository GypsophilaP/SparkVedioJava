import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Created by Gypsophila on 2017/8/31.
 */
//使用本地文件创建RDD
    //统计文本文件字数
public class LocalFile {
    public static void main(String[] args){
        //创建SparkConf
        SparkConf conf = new SparkConf().setAppName("LocalFile").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("hdfs://node1:9000/spark.txt");
        JavaRDD<Integer> lengths = lines.map(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });
        int res = lengths.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println(res + "");
        sc.close();
    }
}
