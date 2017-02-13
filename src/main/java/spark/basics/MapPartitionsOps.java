package spark.basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

/**
 * Created by jcchoiling on 21/1/2017.
 *
 * 随机抽取
 */
public class MapPartitionsOps {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Transformation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> names = sc.parallelize(Arrays.asList("Spark","Scala","Hadoop","Java","Kafka"),3);

        final Map<String, Long> table = new HashMap<String, Long>();
        table.put("Spark", 6L);
        table.put("Scala", 13L);
        table.put("Hadoop", 11L);
        table.put("Java", 23L);
        table.put("Kafka", 10L);

/**
 * mapPartitions
 */
        JavaRDD<Long> ages = names.mapPartitions(new FlatMapFunction<Iterator<String>, Long>() {

            @Override
            public Iterator<Long> call(Iterator<String> partition) throws Exception {
                List<Long> ages = new ArrayList<Long>();

                while (partition.hasNext()){
                    ages.add(table.get(partition.next()));

                }
                return ages.iterator();
            }
        });

        List<Long> agesRes = ages.collect();
        for (Long age: agesRes){
            System.out.println(age);
        }


/**
 * mapPartitionsWithIndex
 */

        JavaRDD<String> classes = names.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

            @Override
            public Iterator<String> call(Integer partitionIndex, Iterator<String> partition) throws Exception {

                List<String> indexed = new ArrayList<String>();
                while(partition.hasNext()){
                    indexed.add(partitionIndex + "_" + partition.next());
                }

                return indexed.iterator();
            }

        },true);

        List<String> classRes = classes.collect();
        for (String classItem: classRes){
            System.out.println(classItem);
        }





        sc.close();
    }
}
