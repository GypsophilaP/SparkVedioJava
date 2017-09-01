import org.apache.commons.collections.ArrayStack;
import org.apache.commons.math3.util.MultidimensionalCounter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import scala.Tuple2;



import java.util.Arrays;
import java.util.Iterator;


/**
 * 本地测试的WordCount程序
 * Created by Gypsophila on 2017/8/29.
 */
//集群模式删掉setMaster

public class WordCountCluster {
    public static void main(String[] args){
        //编写Spark应用程序
        //第一步：创建SparkConf对象，设置Spark应用的配置信息
        //使用setMaster（）可以设置Spark应用程序要链接的Spark集群的Master节点的url；但是设置为local则代表在本地运行
        SparkConf conf = new SparkConf().setAppName("WordCountLocal");
        //第二步创建JavaSparkContext对象
        //在Spark中，SparkContext是Spark所有功能的一个入口，无论用java、scala，甚至python编写都必须要有一个SparkContext
        //他的主要作用，包括初始化Spark应用程序所需的一些核心组件包括调度器（DAGSheduler、TaskScheduler），还会去到Spark Master
        //节点上进行注册等等。
        //SparkContext是Spark应用中一个最重要的对象，在Spark中，编写不同类型的Spark应用使用的SparkContext是不同的，
        //如果使用scala。使用的就是原生的SparkContext对象，使用Java，就是JavaSparkContext对象
        //如果是开发SparkSQL程序，就是SQLContext、HiveContext
        //SparkStreaming 就是SparkStreaming的Coontext 以此类推
        JavaSparkContext sc = new JavaSparkContext(conf);
        //第三步 针对输入源 创建一个出事的RDD
        //输入源中的数据会大三，分配到RDD的每个partition中。
        //SparkContext中，用于根据文件类型的输入源创建RDD的方法，叫做textFile（）方法
        //在Java中，创建普通的RDD，都叫做JavaRDD
        //RDD肿么有元素这种概念，在HDFS或者本地文件，创建的RDD，每一个元素相当于文件里的一行
        JavaRDD<String> lines = sc.textFile("hdfs://node1:9000/spark.txt");
        //第四步 对初始RDD进行transfromatiom操作，也就是计算操作
        //现将每一行拆分成单词
        //FlatMapFunction，有两个泛型参数，分别代表了输入和输出类型
        //在本例中，输入和输出都是String
        //flatMap算子的左右，就是讲RDD的一个元素拆分成多个元素
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        //接着 需要将每一个单词映射为（单词，1）这种形式
        //因为只有这样 后面才能根据单词作为key 来进行单个单词出现次数的累加
        //mapToPair 其实就是将每个元素映射为一个（v1，v2）这样的Tuple2类型的元素 这里的tuple2就是scala类型 包含了两个值
        //mapToPair这个算子 要求的是与PairFunction配合使用，第一个泛型参数代表了输入类型 第二个和第三个代表的是输出的Tuple2的类型
        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });
        //接着需要用单词作为key 统计每个单词出现的次数
        //这里需要用reduceByKey这个算子，对每个key对应的calue都进行reduce操作
        JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //到这里为止 通过spark算子操作已经统计出了单词计数
        //但是之前使用的flatMap mapToPair reduceByKey这种操作 都叫做transformation操作
        //一个spark应喲警钟，光有transformation操作是不行的 不会执行，必须有一种叫action的操作
        //最后可以使用一种叫action的操作比如foreach 来除法程序执行
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println( wordCount._1 + " appeared " + wordCount._2 + " times ");
            }
        });
        sc.close();
    }
}
