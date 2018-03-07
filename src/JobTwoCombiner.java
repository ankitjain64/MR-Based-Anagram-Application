import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * This class will not be required as it does not do anything fruitful for us in case of optimizations!!
 */
public class JobTwoCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> valuesIterable, Context context) throws IOException, InterruptedException {
        for (Text text : valuesIterable) {
            context.write(key, text);
        }
    }
}
