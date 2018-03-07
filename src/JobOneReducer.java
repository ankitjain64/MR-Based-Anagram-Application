import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * A Reducer for job one which pairs up the words that are anagram of each
 * other.
 * Key is NullWritable and value is Text of anagrams separated by a word
 */
public class JobOneReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> valuesIterable, Context context) throws IOException, InterruptedException {
        Text words = Utils.concatText(valuesIterable, Constants.WORD_SEPARATOR);
        context.write(Constants.NULL_WRITABLE, words);
    }


}
