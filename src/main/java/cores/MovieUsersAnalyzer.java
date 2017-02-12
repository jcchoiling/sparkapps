package cores;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;

/**
 * Created by janicecheng on 20/1/2017.
 */
public class MovieUsersAnalyzer {
    public static void main(String[] args){

        String masterURL = "local[1]";
//        String secondary_sorting_data = "src/main/resources/moviedata/dataforsecondarysorting.dat";
        String ratings_data = "src/main/resources/moviedata/ratings.dat";

        /**
         * Create SparkConf and pass the conf to the JavaSparkContext
         */
        SparkConf conf = new SparkConf().setAppName("MovieUsersAnalyzer").setMaster(masterURL);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(ratings_data);

        JavaPairRDD<SecondarySortingKey,String> keyvalues = lines.mapToPair(new PairFunction<String, SecondarySortingKey, String>() {
            @Override
            public Tuple2<SecondarySortingKey, String> call(String line) throws Exception {
                String[] splited = line.split("::");
                SecondarySortingKey key = new SecondarySortingKey(
                        Integer.valueOf(splited[3]),
                        Integer.valueOf(splited[2])
                );
                return new Tuple2<SecondarySortingKey, String>(key, line);
            }
        });

        JavaPairRDD<SecondarySortingKey,String> sorted = keyvalues.sortByKey(false);

        JavaRDD<String> results = sorted.map(new Function<Tuple2<SecondarySortingKey,String>, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(Tuple2<SecondarySortingKey, String> tuple) throws Exception {
                return tuple._2;
            }
        });

//        results.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        List<String> collected = results.take(10);
        for (String item: collected) {
            System.out.println(item);
        }



    }
}
