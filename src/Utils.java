import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * Utils Class containing various methods used across different classes and
 * independent of who calls them. Just does their task gracefully.
 */
public class Utils {

    /**
     * Was initially used during development purpose not used any longer.
     *
     * @param iterable Iterable of text
     * @return array of text
     */
    public static Text[] makeArray(Iterable<Text> iterable) {
        LinkedList<Text> list = new LinkedList<>();
        for (Text item : iterable) {
            list.add(item);
        }
        return list.toArray(new Text[list.size()]);
    }

    /**
     * Concats the word of the in the iterable with the provided separator
     *
     * @param iterable  Iterable of text
     * @param separator Separator to be used while concating the text
     * @return All Text in iterator appended by the separator
     */
    public static Text concatText(Iterable<Text> iterable, String separator) {
        StringBuilder sb = new StringBuilder();
        Iterator<Text> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            String stringValue = iterator.next().toString();
            sb.append(stringValue);
            if (iterator.hasNext()) {
                sb.append(separator);
            }
        }
        return new Text(sb.toString());
    }

    /**
     * Comparator to be used while grouping the keys in stage two. This was
     * done so as to get default descending sort on anagram length. By
     * default it is ascending
     */
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(IntWritable.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable key1 = (IntWritable) w1;
            IntWritable key2 = (IntWritable) w2;
            return -1 * key1.compareTo(key2);
        }


    }

}
