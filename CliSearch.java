import java.util.Vector;
import java.util.List;
import java.util.ArrayList;
import java.util.Enumeration;
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;

public class CliSearch
{
    private static class Tf 
    {
	private String filename;
	private Double tfdf = 0.0;

	public void setFilename(String name) {
	    this.filename = name;
	}

	public void setTfDf(String tf, String df) {
	    Double dtf = Double.valueOf(tf);
	    Double ddf = Double.valueOf(df);
	    Double metric = dtf/ddf;

	    this.tfdf = metric;
	}

	public void setRank(Double r) {
	    this.tfdf = r;
	}

	public String getFile() {
	    return this.filename;
	}

	public Double getRank() {
	    return this.tfdf;
	}
	
	public String toString() {
	    return this.filename + ": " + String.valueOf(this.tfdf);
	}
    }


    public static void main(String[] args) throws IOException{
	if (args.length < 2) {
	    System.out.println("You need to specify a backend and query terms");
	    System.out.println("Example:");
	    System.out.println("java jar client.jar gr.upatras.ceid.romo.CliSearch cassandra");

	    System.exit(1);
	}

	String method = args[0];
	
	if (!method.equals("cassandra") && !method.equals("hbase")) {
	    System.out.println("You need to specify a backend");
	    System.out.println("Example:");
	    System.out.println("java jar client.jar gr.upatras.ceid.romo.CliSearch cassandra");

	    System.exit(1);
	}

	Vector<Tf> answer = new Vector<Tf>();

	if (method.equals("hbase")) {
	    // HBase stuff
	    HBaseConfiguration conf = new HBaseConfiguration();
	    conf.set("hbase.master","romo.ceid.upatras.gr:60000");
	    conf.set("hbase.zookeeper.quorum", "romo.ceid.upatras.gr");
	    HTable htable = new HTable(conf, "lemmas");
	    
	    for (int i = 1; i< args.length; i++) {
		List<Tf> res = hbSearch(args[i], htable);
		if (answer.isEmpty()) {
		    // First term
		    answer.addAll(res);
		} else {
		    // Remove old items
		    Vector<Tf> temp = new Vector<Tf>();
		    for (Enumeration e = answer.elements() ; e.hasMoreElements() ;) {
			Tf toCheck = (Tf) e.nextElement();
			for (int j = 0; j < res.size() ; j++) {
			    if (toCheck.getFile().equals(res.get(j).getFile())) {
				Tf replacement = new Tf();
				replacement.setFilename(toCheck.getFile());
				replacement.setRank(toCheck.getRank() + res.get(j).getRank());

				temp.add(replacement);
			    }
			}
			answer = temp;
		    }
		}
	    }
	    
	    System.out.print("Output: ");
	    Vector<Tf> sorted = new Vector<Tf>();
	    int resultscount = answer.size();
	    for (int i = 0; i < resultscount; i++) {
		Tf max = new Tf();
		for (Enumeration e = answer.elements() ; e.hasMoreElements() ;) {
		    Tf toCheck = (Tf) e.nextElement();
		    
		    if (toCheck.getRank() > max.getRank())
			max = toCheck;
		}
		sorted.add(max);
		answer.remove(max);
	    }
	    for (Enumeration e = sorted.elements(); e.hasMoreElements(); )
		System.out.print(" " + ((Tf) e.nextElement()).getFile());
	    System.out.println("");
	}
    }

    private static List<Tf> hbSearch(String term, HTable ht) throws IOException {
	List<Tf> results = new ArrayList<Tf>();
	Get g = new Get(Bytes.toBytes(term));
	Result r = ht.get(g);
	byte [] value = r.getValue(Bytes.toBytes("df"), Bytes.toBytes("value"));
	String df = Bytes.toString(value);
	
	List<KeyValue> list = r.list();
	for (KeyValue kv: list) {
	    byte[] fam = kv.getFamily();
	    if (Bytes.toString(fam).equals("df"))
		// this is not the family you are looking for
		// move along
		continue;
	    
	    if (Bytes.toString(fam).equals("tf")) {
		    Tf tf = new Tf();
		    tf.setFilename(Bytes.toString(kv.getQualifier()));
		    tf.setTfDf(Bytes.toString(kv.getValue()),df);
		    
		    results.add(tf);
	    }
	}
	return results;
    }
}
