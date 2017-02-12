package basics;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(
        name = "HiveUDF",
        value = "_FUNC_(date) - from the input date string "+
                "or separate month and day arguments, returns the sign of the Zodiac.",
        extended = "Example:\n"
        + " > SELECT _FUNC_(date_string) FROM src;\n" + " > SELECT _FUNC_(month, day) FROM src;"
)


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


