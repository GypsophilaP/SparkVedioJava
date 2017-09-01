import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Gypsophila on 2017/8/30.
 */
public class ParallelizeCollection {
    public static void main(String[] args){
        //创建sparkConf
        SparkConf conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");
        //创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        //通过并行化集合的方式创建RDD 通过调用SparkContext以及其子类的parallelize方法
        List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numsRDD =  sc.parallelize(nums);
        //执行reduce算子操作
        //相当于先进性1 + 2 =3 然后 3 + 3 = 6 以此类推
        int res = numsRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println("" + res);
        //JavaSparkContext需要手动关闭
        sc.close();
    }
}
