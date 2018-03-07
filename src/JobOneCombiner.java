import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner for Job One..takes the output of map task in job one and groups
 * them locally before sending to the reduce task of jobOne
 */
public class JobOneCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> valuesIterable, Context context) throws IOException, InterruptedException {
        Text words = Utils.concatText(valuesIterable, Constants.WORD_SEPARATOR);
        context.write(key, words);
    }
}
