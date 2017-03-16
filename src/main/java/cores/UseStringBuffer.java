package cores;

/**
 * Created by jcchoiling on 7/3/2017.
 */
public class UseStringBuffer {
    public static void  main(String[] args){
        StringBuffer strBuffer = new StringBuffer();
        strBuffer.append(true);
        strBuffer.append("test");
        strBuffer.append('\t');

        for (int i=0; i < 3; i++){
            strBuffer.append(i);
        }

        String str = strBuffer.toString();
//        strBuffer.insert(1,"InsertValue");
        str = strBuffer.toString();
        System.out.println(str);

    }
}
