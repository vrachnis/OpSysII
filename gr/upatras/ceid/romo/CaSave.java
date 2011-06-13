package gr.upatras.ceid.romo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;

public class CaSave
{
    public static class CaMap extends Mapper<LongWritable, Text, Text, Text>
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

    public static class CaReduce extends Reducer<Text, Text, ByteBuffer, List<Mutation>>
    {
	public void reduce(Text lemma, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    List<Mutation> results = new ArrayList<Mutation>();
	    List<Column> columns = new ArrayList<Column>();
	    List<Column> dfc = new ArrayList<Column>();
  	    String toParse = "";
	    long timestamp = System.currentTimeMillis();

	    for (Text report : values)
		toParse = report.toString();

	    String[] metrics = toParse.split(",");
	    String df = metrics[metrics.length -1];
	    // create the df value (single) column
	    Column value = new Column();
	    value.setName("value".getBytes("UTF-8"));
	    value.setValue(df.getBytes("UTF-8"));
	    value.setTimestamp(timestamp);
	    dfc.add(value);

	    for (int i=0; i<metrics.length - 1; i++) {
		String[] metric = metrics[i].replaceAll(",","").split("-");
		String filename = metric[0];
		String tf = metric[1];
		// Add the tf to the columns
		Column btf = new Column();
		btf.setName(filename.getBytes("UTF-8"));
		btf.setValue(tf.getBytes("UTF-8"));
		btf.setTimestamp(timestamp);
		columns.add(btf);
      	    }

	    // TF supercolumn
	    SuperColumn sctf =  new SuperColumn();
	    sctf.setName("tf".getBytes("UTF-8"));
	    sctf.setColumns(columns);
	    ColumnOrSuperColumn cOSC = new ColumnOrSuperColumn();
	    cOSC.super_column = sctf;
	    Mutation mtf = new Mutation();
	    mtf.column_or_supercolumn = cOSC;
	    results.add(mtf);
	    
	    // DF supercolumn
	    SuperColumn scdf = new SuperColumn();
	    scdf.setName("df".getBytes("UTF-8"));
	    scdf.setColumns(dfc);
	    ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
	    cosc.super_column = scdf;
	    Mutation mdf = new Mutation();
	    mdf.column_or_supercolumn = cosc;
	    results.add(mdf);

	    // Commit the change
	    ByteBuffer keyBuffer = ByteBuffer.wrap(lemma.toString().getBytes("UTF-8"));
	    context.write(keyBuffer, results);
	}
    }

    public static void main(String args[]) throws Exception
    {
	Configuration conf = new Configuration();
	String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherargs.length != 1) {
	    System.out.println("You need at 1 argument:");
	    System.out.println("<server>");

	    return;
	}

	Job caSave = new Job(conf,"caSaving the documents");
	caSave.setJarByClass(CaSave.class);
	caSave.setMapperClass(CaMap.class);
	caSave.setReducerClass(CaReduce.class);
	caSave.setMapOutputKeyClass(Text.class);
	caSave.setMapOutputValueClass(Text.class);

	caSave.setOutputFormatClass(ColumnFamilyOutputFormat.class);
	// lemma created by:
	// create column family lemma with column_type = Super and comparator = UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type and subcomparator=UTF8Type;
	ConfigHelper.setOutputColumnFamily(caSave.getConfiguration(), "lemmas", "lemma");

	ConfigHelper.setRpcPort(caSave.getConfiguration(), "9160");
	ConfigHelper.setInitialAddress(caSave.getConfiguration(), otherargs[0]);
	ConfigHelper.setPartitioner(caSave.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");

	Path inputPath = new Path("index");

	FileInputFormat.addInputPath(caSave, inputPath);

	caSave.waitForCompletion(true);
    }
}