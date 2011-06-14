import java.util.Vector;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Enumeration;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

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


    public static void main(String[] args) throws IOException, TException, UnsupportedEncodingException, InvalidRequestException, UnavailableException, TimedOutException {
	if (args.length < 3) {
	    System.out.println("You need to specify a backend and query terms");
	    System.out.println("Example:");
	    System.out.println("java CliSearch.class <cassandra/hbase> <server> <term>");

	    System.exit(1);
	}

	String method = args[0];
	String server = args[1];
	
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
	    conf.set("hbase.master", server + ":60000");
	    conf.set("hbase.zookeeper.quorum", server);
	    HTable htable = new HTable(conf, "lemmas");
	    
	    for (int i = 2; i< args.length; i++) {
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
	} else {
	    TTransport tr = new TFramedTransport(new TSocket(server, 9160));
	    TProtocol proto = new TBinaryProtocol(tr);
	    Cassandra.Client client = new Cassandra.Client(proto);
	    tr.open();

	    List<String> terms = new ArrayList<String>();
	    
	    for (int i=2; i < args.length; i++)
		terms.add(args[i]);
	    
	    List<Tf> ans = caSearch(terms, client);
	    answer.addAll(ans);

	    tr.close();
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

    private static List<Tf> caSearch(List<String> terms, Cassandra.Client c) throws IOException, InvalidRequestException, TException, UnavailableException, TimedOutException {
	List<Tf> results = new ArrayList<Tf>();
	c.set_keyspace("lemmas");
	
	List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
	for (int i=0; i < terms.size(); i++) {
	    keys.add(ByteBuffer.wrap(terms.get(i).getBytes("UTF-8")));
	}
	
	ColumnParent colParent = new ColumnParent("lemma");
	    
	SlicePredicate predicate = new SlicePredicate();
	predicate.addToColumn_names(ByteBuffer.wrap("tf".getBytes("UTF-8")));
	predicate.addToColumn_names(ByteBuffer.wrap("df".getBytes("UTF-8")));
	
	Map<ByteBuffer, List<ColumnOrSuperColumn>> map = c.multiget_slice(keys, colParent, predicate, ConsistencyLevel.ONE);

	for (ByteBuffer key : keys) {
	    List<Tf> temp = new ArrayList<Tf>();
	    List<ColumnOrSuperColumn> list = map.get(key);
	    String df = new String(list.get(0).super_column.getColumns().get(0).getValue(), "UTF-8");
	    for (ColumnOrSuperColumn cs : list) {
		SuperColumn sc = cs.super_column;
		    if ((new String(sc.getName(), "UTF-8")).equals("tf")) {
			List<Column> cols = sc.getColumns();
			for (Column col: cols){
			    Tf result = new Tf();
			    String file = new String(col.getName(), "UTF-8");
			    String tf = new String(col.getValue(), "UTF-8");
			    
			    result.setFilename(file);
			    result.setTfDf(tf,df);

			    temp.add(result);
			}
		    }
		    
	    }
	    if (results.size() == 0) 
		results = temp;
	    else {
		// Remove old items
		for (int i = 0; i < results.size(); i++) {
		    String found = "NO";
		    for (int j = 0; j < temp.size(); j++)
			if (temp.get(j).getFile().equals(results.get(i).getFile())) {
			    found = "YES";
			    Tf replacement = new Tf();
			    replacement.setFilename(results.get(i).getFile());
			    replacement.setRank(results.get(i).getRank() + temp.get(j).getRank());

			    results.remove(i);
			    results.add(i, replacement);
			}
		    
		    if (found.equals("NO")) {
			results.remove(i);
			i--;
		    }
		}
	    }
	}

	return results;
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
