package gr.upatras.ceid.romo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HBSave
{
    public static class SMap extends Mapper<LongWritable, Text, Text, Text>
    {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);

	    String lemma;
	    if (tokenizer.countTokens() == 1)
		lemma = " ";
	    else
		lemma = tokenizer.nextToken();
	    
	    String metrics = tokenizer.nextToken().trim();
	    
	    context.write(new Text(lemma), new Text(metrics));
      	}
    }

    public static class SReduce extends TableReducer<Text, Text, ImmutableBytesWritable>
    {
	public void reduce(Text lemma, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    String toParse = "";

	    for (Text report : values) {
		toParse = report.toString();
	    }

	    // New row
	    Put p = new Put(Bytes.toBytes(lemma.toString()));

	    String[] metrics = toParse.split(",");
	    String df = metrics[metrics.length -1];
	    p.add(Bytes.toBytes("df"), Bytes.toBytes("value"), Bytes.toBytes(df));
	    for (int i=0; i<metrics.length - 1; i++) {
		String[] metric = metrics[i].replaceAll(",","").split("-");
		String filename = metric[0];
		String tf = metric[1];
		p.add(Bytes.toBytes("tf"), Bytes.toBytes(filename), Bytes.toBytes(tf));
      	    }

	    ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(lemma.toString()));
	    context.write(key, p);
	}
    }

    public static void main(String args[]) throws Exception
    {
	HBaseConfiguration conf = new HBaseConfiguration();
	conf.set("hbase.master","romo.ceid.upatras.gr:60000");
	conf.set("hbase.zookeeper.quorum", "romo.ceid.upatras.gr");
	String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherargs.length < 1) {
	    System.out.println("You need at least 1 argument:");
	    System.out.println("<server>");

	    return;
	}

	Job hbSave = new Job(conf,"hbSaving the documents");
	hbSave.setJarByClass(HBSave.class);
	hbSave.setMapperClass(SMap.class);
	TableMapReduceUtil.initTableReducerJob("lemmas", SReduce.class, hbSave);
	hbSave.setMapOutputKeyClass(Text.class);
	hbSave.setMapOutputValueClass(Text.class);

	Path inputPath = new Path("index");

	FileInputFormat.addInputPath(hbSave, inputPath);

	hbSave.waitForCompletion(true);
    }
}