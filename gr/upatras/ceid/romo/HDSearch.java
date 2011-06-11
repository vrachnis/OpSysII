package gr.upatras.ceid.romo;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HDSearch
{
    public static class SMap extends Mapper<LongWritable, Text, Text, DoubleWritable>
    {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String[] terms = context.getConfiguration().getStrings("terms");
	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    
	    String lineKey = tokenizer.nextToken();

	    for (String term : terms) {
		if (term.equals(lineKey)) {
		    String stats = tokenizer.nextToken();
		    String[] metrics = stats.split(",");
		    Double df = new Double(metrics[metrics.length -1]);
		    for (int i=0; i<metrics.length - 1; i++) {
			String[] metric = metrics[i].replaceAll(",","").split("-");
			String textfile = metric[0];
			String tf = metric[1];
			context.write(new Text(textfile), new DoubleWritable(Double.valueOf(tf)/df));
		    }
		    break;
		}
	    }
	}
    }

    public static class SReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
	public void reduce(Text file, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
	    Double relativity = 0.0;
	    
	    for (DoubleWritable metric : values) {
		relativity += metric.get();
	    }

	    context.write(file, new DoubleWritable(relativity));
	}
    }

    public static void main(String args[]) throws Exception
    {
	Configuration conf = new Configuration();
	String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherargs.length < 1) {
	    System.out.println("You need at least 1 argument:");
	    System.out.println("<query> ... <query>");

	    return;
	}

	Job search = new Job(conf,"search");
	search.setJarByClass(HDSearch.class);
	search.setMapperClass(SMap.class);
	search.setReducerClass(SReduce.class);
	search.setOutputKeyClass(Text.class);
	search.setOutputValueClass(DoubleWritable.class);
	//search.setNumReduceTasks(5);

	Path inputPath = new Path("index");
	Path outputPath = new Path("results");
	FileSystem fs = inputPath.getFileSystem(conf);
	if (fs.exists(outputPath))
	    fs.delete(outputPath, true);

	FileInputFormat.addInputPath(search, inputPath);
	FileOutputFormat.setOutputPath(search, outputPath);

	search.getConfiguration().setStrings("terms", otherargs);

	search.waitForCompletion(true);
    }
}