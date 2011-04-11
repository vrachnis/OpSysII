package gr.upatras.ceid.romo;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Idf
{
    public static class IdfMap extends Mapper<LongWritable, Text, Text, Text>
    {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    String[] splitter = line.split("\\s+");

	    context.write(new Text(splitter[0]), new Text(splitter[1]+ " " +splitter[2]));
	}
    }

    public static class IdfReduce extends Reducer<Text, Text, Text, MapWritable>
    {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    Map<String, String> outputMap = new HashMap<String, String>();
	    IntWritable filesNo = new IntWritable(context.getConfiguration().getInt("filesNo", 1));
	    int docsCount = 0;

	    for (Text tf : values) {
		docsCount++;
		outputMap.put(tf.toString().split(" ")[1], tf.toString().split(" ")[0]);
	    }

	    MapWritable outMap = new MapWritable();
	    for (Map.Entry<String, String> entry : outputMap.entrySet()) {
		DoubleWritable tf = new DoubleWritable(Double.parseDouble(entry.getValue()));
		DoubleWritable idf = new DoubleWritable(Math.log10(((double) filesNo.get()) / docsCount));
		DoubleWritable tfidf = new DoubleWritable(tf.get()*idf.get());
		MapWritable docMap = new MapWritable();

		docMap.put(new Text("tf"), tf);
		docMap.put(new Text("idf"), idf);
		docMap.put(new Text("tfidf"), tfidf);
		outMap.put(new Text(entry.getKey()),docMap);
	    }

	    context.write(key, outMap);
	}
    }

    public static void main(String[] args) throws Exception 
    {
	Configuration conf = new Configuration();
//      String otherargs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	Job idf = new Job(conf, "IDF");
	idf.setJarByClass(Idf.class);
	idf.setMapperClass(IdfMap.class);
	idf.setReducerClass(IdfReduce.class);
	idf.setMapOutputKeyClass(Text.class);
	idf.setMapOutputValueClass(Text.class);
	idf.setOutputKeyClass(Text.class);
	idf.setOutputFormatClass(SequenceFileOutputFormat.class);
	idf.setOutputValueClass(MapWritable.class);

	Path inputPath = new Path("altq");
	Path outputPath = new Path("altqidf");
	Path middlePath = new Path("tf-temp");
	FileSystem fs = inputPath.getFileSystem(conf);
	FileStatus[] stat = fs.listStatus(inputPath);
	int filesNo = stat.length;

	FileInputFormat.addInputPath(idf, middlePath);
        FileOutputFormat.setOutputPath(idf, outputPath);

	idf.getConfiguration().setInt("num", filesNo);

	idf.setJobName("TF-IDF for " + String.valueOf(filesNo) + " files");
	idf.waitForCompletion(true);
    }
}
