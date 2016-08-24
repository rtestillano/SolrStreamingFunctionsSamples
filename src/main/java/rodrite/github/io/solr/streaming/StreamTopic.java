package rodrite.github.io.solr.streaming;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.DaemonStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TopicStream;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;

public class StreamTopic 
{
    public static void main( String[] args ) 
    {
    	String zookeeperHost = "localhost:9983";
    	StreamContext context = new StreamContext();
		SolrClientCache cache = new SolrClientCache();
		context.setSolrClientCache(cache);
    			 
		Map<String, String[]> topicQueryParams = new HashMap<String, String[]>(); 
		topicQueryParams.put("q",new String[]{"Red"});    // The query for the topic
		topicQueryParams.put("rows", new String[]{"500"});// How many rows to fetch during each run
		topicQueryParams.put("fl", new String[]{"*"});    // The field list to return with the documents
		
		SolrParams solrPararms =  new MultiMapSolrParams(topicQueryParams);
		 
		TopicStream topicStream = new TopicStream(zookeeperHost,         // Host address for the zookeeper service housing the collections 
		                                         "checkpoints",   // The collection to store the topic checkpoints
		                                         "gettingstarted",// The collection to query for the topic records
		                                         "topicId",       // The id of the topic
		                                         -1,              // checkpoint every X tuples, if set -1 it will checkpoint after each run.
		                                         solrPararms);    // The query parameters for the TopicStream
		 
		DaemonStream daemonStream = new DaemonStream(topicStream, // The underlying stream to run. 
		                                             "prueba",    // The id of the daemon
		                                             1000,        // The interval at which to run the internal stream
		                                             500);        // The internal queue size for the daemon stream. Tuples will be placed in the queue
		                                                          // as they are read by the internal internal thread.
		                                                          // Calling read() on the daemon stream reads records from the internal queue.
		                                                                        
		daemonStream.setStreamContext(context);
		daemonStream.open();
		 
		workWithTuples(daemonStream); //here we work with the Tuples
		
		daemonStream.close();
    }

	private static void workWithTuples(DaemonStream daemonStream) {
		while(true) {
		    Tuple tuple;
			try {
				tuple = daemonStream.read();
			    if(tuple.EOF) {
			        break;
			    } else {
			        System.out.println(tuple.fields.toString());
			    }
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
