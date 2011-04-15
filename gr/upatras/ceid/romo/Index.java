package gr.upatras.ceid.romo;

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

public class Index
{

    public static class IIMap extends Mapper<LongWritable, Text, Text, Text>
    {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    FileSplit filesplit = (FileSplit)context.getInputSplit();
	    String filename = filesplit.getPath().getName();

	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    while (tokenizer.hasMoreTokens()) {
		context.write(new Text(tokenizer.nextToken()), new Text(filename));
	    }
	}
    }

    public static class IIReduce extends Reducer<Text, Text, Text, Text>
    {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    Map<String, Integer> wordMap = new HashMap<String, Integer>();

	    for (Text filename : values) {
		if (! wordMap.containsKey(filename.toString()))
		    wordMap.put(filename.toString(), 1);
	    }

	    String index = "";
	    for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
		index += entry.getKey() + ", ";
	    }
	    context.write(new Text(key), new Text(index.replaceAll(", $", "").replaceAll(".txt","")));
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

        Job iIndex = new Job(conf, otherargs[2] + " inverted index calculation");
	iIndex.setJarByClass(Index.class);
        iIndex.setMapperClass(IIMap.class);
        iIndex.setReducerClass(IIReduce.class);
        iIndex.setOutputKeyClass(Text.class);
        iIndex.setOutputValueClass(Text.class);
	iIndex.setNumReduceTasks(5);

	Path inputPath = new Path(otherargs[0]);
	Path outputPath = new Path(otherargs[1]);

        FileInputFormat.addInputPath(iIndex, inputPath);
        FileOutputFormat.setOutputPath(iIndex, outputPath);

	iIndex.waitForCompletion(true);
    }
}
