package gr.upatras.ceid.romo;

import gr.upatras.ceid.romo.Idf.*;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Tf
{

    public static class TfMap extends Mapper<LongWritable, Text, Text, Text>
    {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    FileSplit filesplit = (FileSplit)context.getInputSplit();
	    String filename = filesplit.getPath().getName();

	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    while (tokenizer.hasMoreTokens()) {
		context.write(new Text(filename), new Text(tokenizer.nextToken()));
	    }
	}
    }

    public static class TfReduce extends Reducer<Text, Text, Text, Text>
    {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    int totalWords = 0;
	    Map<String, Integer> wordMap = new HashMap<String, Integer>();

	    for (Text word : values) {
		totalWords++;
		if (wordMap.containsKey(word.toString()))
		    wordMap.put(word.toString(), wordMap.get(word.toString()) + 1);
		else
		    wordMap.put(word.toString(), 1);
	    }

	    for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
		Text val = new Text(String.valueOf(((double) entry.getValue())/totalWords) + " " + key.toString());
		//		System.out.println(entry.getKey() + " : " + val.toString());
		context.write(new Text(entry.getKey()), val);
	    }
	}
    }
    
    public static void main(String args[]) throws Exception
    {
        Configuration conf = new Configuration();
	String otherargs[] = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherargs.length != 3) {
	    System.out.println("You need to supply 3 arguments:");
	    System.out.println("<input> <output> <title>");

	    return;
	}

        Job tf = new Job(conf, otherargs[2] + " TF calculation");
	tf.setJarByClass(Tf.class);
        tf.setMapperClass(TfMap.class);
        tf.setReducerClass(TfReduce.class);
        tf.setOutputKeyClass(Text.class);
        tf.setOutputValueClass(Text.class);
	tf.setNumReduceTasks(5);

	Path inputPath = new Path(otherargs[0]);
	Path outputPath = new Path(otherargs[1]);
	Path middlePath = new Path("tf-temp");
	FileSystem fs = inputPath.getFileSystem(conf);
	FileStatus[] stat = fs.listStatus(inputPath);
	int filesNo = stat.length;

	if (fs.exists(middlePath))
	    fs.delete(middlePath, true);

        FileInputFormat.addInputPath(tf, inputPath);
        FileOutputFormat.setOutputPath(tf, middlePath);

	tf.waitForCompletion(true);

	conf = new Configuration();
	Job idf = new Job(conf, "IDF");
	idf.setJarByClass(Idf.class);
	idf.setMapperClass(IdfMap.class);
	idf.setReducerClass(IdfReduce.class);
	idf.setMapOutputKeyClass(Text.class);
	idf.setMapOutputValueClass(Text.class);
	idf.setOutputKeyClass(Text.class);
	//idf.setOutputFormatClass(SequenceFileOutputFormat.class);
	idf.setOutputValueClass(Text.class);
	idf.setNumReduceTasks(5);

	FileInputFormat.addInputPath(idf, middlePath);
        FileOutputFormat.setOutputPath(idf, outputPath);

	idf.getConfiguration().setInt("filesNo", filesNo);

	idf.setJobName(otherargs[2] + " TF-IDF calculation");
	//idf.addDependingJob(tf);

	idf.waitForCompletion(true);
    }
}
