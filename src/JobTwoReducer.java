import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JobTwoReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

    private static final NullWritable NULL_WRITABLE = NullWritable.get();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> valuesIterable, Context context) throws IOException, InterruptedException {
        for (Text text : valuesIterable) {
            context.write(NULL_WRITABLE, text);
        }
    }
}
