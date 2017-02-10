package basics;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by janicecheng on 10/2/2017.
 */
public final class HiveUDF extends UDF{
    public Text evaluate(final Text s){
        if (s == null) {
            return null;
        }
        return new Text(s.toString() + " UDF Function");
    }
}


