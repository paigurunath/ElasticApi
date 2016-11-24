

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;

import com.ge.datasource.DBQueryInteract;
import com.google.gson.Gson;

public class Elastic {

	
	static Logger log = Logger.getLogger(Elastic.class.getName());
	private Properties properties = null;
	private Client client;
	private static String serverName;

	public Elastic() {
		this(new Properties());
	}
	
	public Elastic(Properties properties) {
		this.properties = properties;
		serverName = this.properties.getProperty("elastic.environment");
	}

	public void closeES(Client client) {
		log.info("close callled " + client);
		if (client != null) {
			client.close();
		}
	}

	public Client getESClient() {
		try {

			if (this.client != null) {
				log.info("No new client created");
				return this.client;
			}

			TransportClient client = TransportClient.builder()
					.settings(Settings.builder()
					.put(this.properties.getProperty("elastic.cluster.name"),
							this.properties.getProperty("elastic.cluster.myname")))
					.build();
	
			if (serverName.equals("DEV")) {
				return client.addTransportAddresses(
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.dev.host1"),
										Integer.parseInt(this.properties.getProperty("elastic.dev.host1.port2")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.dev.host1"),
								Integer.parseInt(this.properties.getProperty("elastic.dev.host1.port3")))
						));
			} else if (serverName.equals("STAGE")) {
				return client.addTransportAddresses(
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.stage.host1"),
								Integer.parseInt(this.properties.getProperty("elastic.stage.host1.port1")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.stage.host1"),
								Integer.parseInt(this.properties.getProperty("elastic.stage.host1.port2")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.stage.host1"),
								Integer.parseInt(this.properties.getProperty("elastic.stage.host1.port3")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.stage.host2"),
								Integer.parseInt(this.properties.getProperty("elastic.stage.host2.port1")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.stage.host2"),
								Integer.parseInt(this.properties.getProperty("elastic.stage.host2.port2"))))
						);
			} else if (serverName.equals("PROD")) {
				return client.addTransportAddresses(
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.prod.host1"),
								Integer.parseInt(this.properties.getProperty("elastic.prod.host1.port1")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.prod.host2"),
								Integer.parseInt(this.properties.getProperty("elastic.prod.host2.port1")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.prod.host3"),
								Integer.parseInt(this.properties.getProperty("elastic.prod.host3.port1")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.prod.host4"),
								Integer.parseInt(this.properties.getProperty("elastic.prod.host4.port1")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.prod.host5"),
								Integer.parseInt(this.properties.getProperty("elastic.prod.host5.port1"))))
						);
			} else if (serverName.equals("LOCAL1")) {
				return client.addTransportAddresses(
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.local1.host1"),
								Integer.parseInt(this.properties.getProperty("elastic.local1.host1.port1")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.local1.host1"),
								Integer.parseInt(this.properties.getProperty("elastic.local1.host1.port2")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.local1.host1"),
								Integer.parseInt(this.properties.getProperty("elastic.local1.host1.port3")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.local1.host1"),
								Integer.parseInt(this.properties.getProperty("elastic.local1.host1.port4")))),
						new InetSocketTransportAddress(
								new InetSocketAddress(this.properties.getProperty("elastic.local1.host1"),
								Integer.parseInt(this.properties.getProperty("elastic.local1.host1.port5"))))
						);
			} else if (serverName.equals("LOCAL")) {
				return client.addTransportAddress(
						new InetSocketTransportAddress(
								new InetSocketAddress("localhost", 9300)));
			}
		} catch(Exception e) {
			log.error("error occured get client::::" + e);
		}
		return null;
	}
	
	/* Add document to Elastic search index */
	public boolean addDocToElSearch(Map<String, Object> inputVal,String id,Client client) throws ElasticsearchException{
		try {
			Gson gson = new Gson();
			gson.toJson(inputVal);
			JSONObject jsonObj = new JSONObject(inputVal);
			jsonObj.remove("_id");
			
			IndexResponse response = client.prepareIndex(this.properties.getProperty("elastic.index.name"), this.properties.getProperty("elastic.index.type"), id)
					.setSource(jsonObj.toString())
					.get();
			
			log.info("document added to ES: " + response);
		
			return true;
		} catch (ElasticsearchException e) {
			log.error("error occured while adding document in elasticSearch:::::" + e.getMessage());
			throw new ElasticsearchException(e.getMessage());
		}
	}

	/* Add document to Elastic search index */
	public boolean addDocumentToElSearch(Map<String, Object> inputVal,String id,BulkRequestBuilder bulkRequest,Client client) throws ElasticsearchException{
		try {
			Gson gson = new Gson();
			gson.toJson(inputVal);
			JSONObject jsonObj = new JSONObject(inputVal);
			jsonObj.remove("_id");

			// either use client#prepare, or use Requests# to directly build index/delete requests
			bulkRequest.add(client.prepareIndex(this.properties.getProperty("elastic.index.name"), this.properties.getProperty("elastic.index.type"), id)
			        .setSource(jsonObj.toString())
			        );

			return true;
		} catch (ElasticsearchException e) {
			System.out.println("error occured while adding document in elasticSearch:::::" + e.getMessage());
			throw new ElasticsearchException(e.getMessage());
		}
	}

	/* Add document to Elastic search index */
	public boolean upsertDocToElSearch(Map<String, Object> inputVal,String id,Client client) throws ElasticsearchException{
		try {
			/*Gson gson = new Gson();
			gson.toJson(inputVal);*/
			JSONObject jsonObj = new JSONObject(inputVal);
			jsonObj.remove("_id");
			
			//create the indexrequest object to index new documents from mylinks
			IndexRequest indexRequest = new IndexRequest();
			indexRequest.index(this.properties.getProperty("elastic.index.name"));
			indexRequest.type(this.properties.getProperty("elastic.index.type"));
			indexRequest.id(id);
			indexRequest.source(jsonObj.toString());
			
			//create a updaterequest object to update the existing documents
			UpdateRequest updateRequest = new UpdateRequest();
			updateRequest.index(this.properties.getProperty("elastic.index.name"));
			updateRequest.type(this.properties.getProperty("elastic.index.type"));
			updateRequest.id(id);
			
			updateRequest.doc(jsonObj.toString());
			updateRequest.upsert(indexRequest);

			client.update(updateRequest).get();
			
			return true;
		} catch (ElasticsearchException e) {
			log.error("error occured while adding document in elasticSearch:::::" + e.getMessage());
			throw new ElasticsearchException(e.getMessage());
		} catch(Exception e) {
			
		}
		return false;
	}
	public String deleteDocument(String id, String indexname, String type, Client client){
		String result = null;

		DeleteResponse response = client.prepareDelete(indexname, type, id)
				.execute()
				.actionGet();
		if(response.isFound()){
			result = "deleted";
		}else{
			result = "Document not found in index";
		}
		return result;
	}
	
	public void getSpecifiedFieldList(List idList, Client client){
		//client = getESClient();
		SearchResponse response = client.prepareSearch(this.properties.getProperty("elastic.index.name"))
		   .setTypes(this.properties.getProperty("elastic.index.type"))
		   .setQuery(QueryBuilders.matchAllQuery()).setSize(5000)
		   .execute()
		   .actionGet();
		
		SearchHit[] results = response.getHits().getHits();
		
		Map<String,Object> result = null;
		for (SearchHit hit : results) {
			result = hit.getSource();
			String result_id = hit.getId();
			
			if(result_id.charAt(0) != 'P')
			idList.add(result_id);
			/*JSONObject jobj = new JSONObject(result);
			System.out.println(jobj.get("_id"));*/
			//System.out.println(result_id);
		}	
	}

}
