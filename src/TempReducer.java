import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TempReducer extends Reducer<Integer, Integer, NullWritable,
        Integer> {
    List<Tuple> tuples = new ArrayList<>();

    @Override
    public void run(Reducer<Integer, Integer, NullWritable, Integer>.Context
                            context) throws IOException, InterruptedException {
        this.setup(context);

        try {
            while (context.nextKey()) {

            }
            //
        } finally {
            this.cleanup(context);
        }

    }


    static class Tuple {
        private int key;
        private int count;

        public Tuple(int key, int count) {
            this.key = key;
            this.count = count;
        }
    }
}
