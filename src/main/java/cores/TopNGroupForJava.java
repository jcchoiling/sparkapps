package cores;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by jcchoiling on 18/2/2017.
 */
public class TopNGroupForJava {

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setAppName("MovieUsersAnalyzer").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("src/main/resources/general/topNGroup.txt");

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] splitedLine = line.split(" ");
                return new Tuple2<String, Integer>(splitedLine[0], Integer.valueOf(splitedLine[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupedPairs =  pairs.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> top5 = groupedPairs.mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> groupedData) throws Exception {

                Integer[] top5 = new Integer[5];
                String groupedKey = groupedData._1;
                Iterator<Integer> groupedValue = groupedData._2.iterator();

                while(groupedValue.hasNext()){

                    Integer value = groupedValue.next();

                    for (int i = 0; i < 5; i++){
                        if (top5[i] == null){
                            top5[i] = value;
                            break;
                        } else if (value > top5[i]){
                            for (int j = 4; j > i; j--){
                                top5[j] = top5[j-1];
                            }
                            top5[i] = value;
                            break;
                        }
                    }

                }

                return new Tuple2<String,Iterable<Integer>> (groupedKey, Arrays.asList(top5));
            }
        });

        top5.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>(){
            @Override
            public void call(Tuple2<String,Iterable<Integer>> topped) throws Exception {


                System.out.println("Group Key : " + topped._1);
                Iterator<Integer> topValue = topped._2.iterator();

                while (topValue.hasNext()){
                    Integer value = topValue.next();
                    System.out.println(value);
                }

                System.out.println("**********************************************");


            }
        });


        sc.close();




    }

}
