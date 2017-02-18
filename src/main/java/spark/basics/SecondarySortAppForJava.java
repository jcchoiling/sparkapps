package spark.basics;

import cores.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created by jcchoiling on 18/2/2017.
 */
public class SecondarySortAppForJava {
    public static void  main(String[] args){

        SparkConf conf = new SparkConf().setAppName("SecondarySortAppForJava").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("src/main/resources/general/spark.txt");

        JavaPairRDD<cores.SecondarySort, String> pairs = lines.mapToPair(new PairFunction<String, cores.SecondarySort, String>() {

            @Override
            public Tuple2<cores.SecondarySort, String> call(String line) throws Exception {
                String[] splitted = line.split(" ");
                cores.SecondarySort key = new cores.SecondarySort(Integer.valueOf(splitted[0]),Integer.valueOf(splitted[1]));
                return new Tuple2<cores.SecondarySort, String>(key, line);
            }
        });

        JavaPairRDD<cores.SecondarySort, String> sorted = pairs.sortByKey(false);

        JavaRDD<String> SecondarySorted = sorted.map(new Function<Tuple2<cores.SecondarySort,String>, String>() {
            @Override
            public String call(Tuple2<cores.SecondarySort, String> sortedContent) throws Exception {
                return sortedContent._2;
            }
        });

        SecondarySorted.foreach(new VoidFunction<String>(){
            @Override
            public void call(String sorted) throws Exception {

                System.out.println(sorted);

            }
        });


    }
}
