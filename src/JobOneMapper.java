import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


/**
 * Author: Group 1
 * Course: CS744
 * Reads the input file And returns sorted text as the key
 * for the word (so that they can
 * be grouped together) on that line (each line has single word initially)
 * and value as the actual word. This will taken in by combiner / reducer to
 * generated intermediate output file
 */
public class JobOneMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String inputString = value.toString();
            char[] inputCharArray = inputString.toCharArray();
            Arrays.sort(inputCharArray);
            context.write(new Text(String.valueOf(inputCharArray)), value);
        }
    }
}
