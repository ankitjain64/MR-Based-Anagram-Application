import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Date;

/**
 * Author: Group 1
 * Course: CS744
 * Creates two jobs One for reading the word list from the hdfs and grouping
 * anagrams together and write it to hdfs as intermediate output
 * Second job reads this intermediate file and sorts it and generates the
 * final output path.
 */
public class AnagramSorter {

    private static final String INTERMEDIATE_FILE_PATH = "/anagram_sorter_intermediate";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here
        Configuration confOne = new Configuration();

        long currentTime = new Date().getTime();
        Job jobOne = Job.getInstance(confOne, "anagramCount-" + currentTime);
//        jobOne.setJar("ac.jar");
        jobOne.setJarByClass(AnagramSorter.class);

        //Set Input and output class for the job
        jobOne.setInputFormatClass(TextInputFormat.class);
        jobOne.setOutputFormatClass(TextOutputFormat.class);
//        Map Output Key And Value Class
        jobOne.setMapOutputKeyClass(Text.class);
        jobOne.setMapOutputValueClass(Text.class);
        //Reducer OutPut Key And Value..also same for Map by default
        jobOne.setOutputKeyClass(NullWritable.class);
        jobOne.setOutputValueClass(Text.class);

        //Set Mapper,Reducer and Combiner Class
        jobOne.setMapperClass(JobOneMapper.class);
        jobOne.setCombinerClass(JobOneCombiner.class);
        jobOne.setReducerClass(JobOneReducer.class);

        FileInputFormat.addInputPath(jobOne, new Path(args[0]));
        String interimPathString = INTERMEDIATE_FILE_PATH + "_" + currentTime;
        FileOutputFormat.setOutputPath(jobOne, new Path(interimPathString));

        boolean isSuccess = jobOne.waitForCompletion(true);
        if (!isSuccess) {
            System.exit(-1);
        }

        Configuration confTwo = new Configuration();

        Job jobTwo = Job.getInstance(confTwo, "anagramSort-" + currentTime);
//        jobTwo.setJar("ac.jar");
        jobTwo.setJarByClass(AnagramSorter.class);

        //Map OutPut Class Types
        jobTwo.setMapOutputKeyClass(IntWritable.class);
        jobTwo.setMapOutputValueClass(Text.class);
        //Reducer Output Class
        jobTwo.setOutputKeyClass(NullWritable.class);
        jobTwo.setOutputValueClass(Text.class);

        jobTwo.setMapperClass(JobTwoMapper.class);
//        jobTwo.setCombinerClass(JobTwoReducer.class); //Not Required ..does not provide any optimizations
        jobTwo.setReducerClass(JobTwoReducer.class);

        jobTwo.setInputFormatClass(TextInputFormat.class);
        jobTwo.setOutputFormatClass(TextOutputFormat.class);

        jobTwo.setSortComparatorClass(Utils.KeyComparator.class);

        FileInputFormat.addInputPath(jobTwo, new Path(interimPathString));
        FileOutputFormat.setOutputPath(jobTwo, new Path(args[1]));

        isSuccess = jobTwo.waitForCompletion(true);
        if (!isSuccess) {
            System.exit(-1);
        }
        System.exit(0);

    }
}
