package spark.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jcchoiling on 21/1/2017.
 *
 *
 * 它是相当于 SQL 的 full outer join
 * Output results:
 * (a,1)
 * (a,2)
 * (a,3)
 * (b,1)
 * (b,2)
 * (b,3)
 */
public class CartesianOps {

    public static void main(String[] args ){
        SparkConf conf = new SparkConf().setAppName("Transformation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> a = Arrays.asList("a","b");
        List<Integer> b = Arrays.asList(1,2,3);

        JavaRDD<String> aRDD = sc.parallelize(a);
        JavaRDD<Integer> bRDD = sc.parallelize(b);

        JavaPairRDD<String, Integer> result = aRDD.cartesian(bRDD);

        for (Tuple2<String, Integer> item: result.collect()){
            System.out.println(item);
        }

        sc.close();

    }
}
