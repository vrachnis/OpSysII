package gr.upatras.ceid.romo;

import java.util.regex.Pattern;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Enumeration;

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

public class Sort
{
    public static class SortMap extends Mapper<LongWritable, Text, Text, Text>
    {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    
	    String fname = tokenizer.nextToken();
	    String tfdf = tokenizer.nextToken();
	    context.write(new Text("output"), new Text(tfdf + "-" + fname));
	}
    }

    public static class SortReduce extends Reducer<Text, Text, Text, Text>
    {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    Map<String, Double> outputMap = new HashMap<String, Double>();
	    String answer = "";

	    for (Text file : values) {
		String[] dim = file.toString().split(Pattern.quote("-"));
		outputMap.put(dim[1], Double.valueOf(dim[0].replaceAll("-","")));
	    }

	    while (!outputMap.isEmpty()) {
		Double max = 0.0;
		String maxtext = "";
		for (String keymap : outputMap.keySet())
		    if (outputMap.get(keymap) > max) {
			max = outputMap.get(keymap);
			maxtext = keymap;
		    }

		if (answer.equals(""))
		    answer = maxtext;
		else
		    answer += "," + maxtext;

		outputMap.remove(maxtext);
	    }

	    if (answer.equals(""))
		answer = "Nothing relative found";
	    
	    context.write(key, new Text(answer));
	}
    }

    public static void main(String args[]) throws Exception
    {
	Configuration conf = new Configuration();
	String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();

	Job search = new Job(conf,"sorting results");
	search.setJarByClass(Sort.class);
	search.setMapperClass(SortMap.class);
	search.setReducerClass(SortReduce.class);
	search.setOutputKeyClass(Text.class);
	search.setOutputValueClass(Text.class);

	Path inputPath = new Path("results");
	Path outputPath = new Path("sorted");
	FileSystem fs = inputPath.getFileSystem(conf);
	if (fs.exists(outputPath))
	    fs.delete(outputPath, true);

	FileInputFormat.addInputPath(search, inputPath);
	FileOutputFormat.setOutputPath(search, outputPath);

	search.waitForCompletion(true);
    }
}