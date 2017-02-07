package basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jcchoiling on 21/1/2017.
 *
 * 随机抽取
 */
public class SampleOps {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Transformation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> names = sc.parallelize(Arrays.asList("Spark","Scala","Hadoop","Java","Kafka"));

        JavaRDD<String> luckyDraw = names.sample(false, 0.5, 5);
//        for (String item: luckyDraw.collect()){
//            System.out.println(item);
//        }


        List<String> results = names.takeSample(false, 4);
        for (String item: results){
            System.out.println(item);
        }


        sc.close();
    }
}
