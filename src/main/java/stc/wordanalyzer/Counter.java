package stc.wordanalyzer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class Counter {

	public Counter() {
		// TODO Auto-generated constructor stub
	}

	protected Client makeClient() throws IOException {
		String clusterName = "elasticsearch";
		String host = "127.0.0.1";
		int port = 9300;

		Settings.Builder settingsBuilder = Settings.settingsBuilder();
		settingsBuilder.put("cluster.name", clusterName);
		Settings settings = settingsBuilder.build();

		Client client = null;
		TransportClient transportClient = TransportClient.builder().settings(settings).build();

		transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
		client = transportClient;

		return client;
	}

	public static void main(String[] args) {

		Counter counter = new Counter();
	
		Client client = null;
		try {
			client = counter.makeClient();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (client != null) {
			System.out.println("you made it");
			counter.statisticTerms(client);
		}
	}

	public void statisticTerms(Client client) {

		Map<String, Integer> totalTermCounts = new HashMap<String, Integer>();

		String index = "nutch";
		String type = "doc";
		SearchResponse scrollResp = client.prepareSearch(index).setTypes(type).setQuery(QueryBuilders.matchAllQuery())
				.setScroll(new TimeValue(60000)).setSize(100).execute().actionGet();

		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> result = hit.getSource();

				Set<String> pageTerms = new HashSet<String>();
				String inlinks = (String) result.get("anchor_inlinks");
				String outlinks = (String) result.get("anchor_outlinks");
				
				String allLinks = inlinks + "&&" + outlinks;
				String[] linksTemrs = allLinks.split("&&");
				for (String term : linksTemrs) {
					term = term.trim();
					if(!term.isEmpty()){
						pageTerms.add(term);
					}
				}
				
				for (String term : pageTerms) {
					if (totalTermCounts.containsKey(term)) {
						int count = totalTermCounts.get(term);
						totalTermCounts.put(term, count + 1);
					} else {
						totalTermCounts.put(term, 1);
					}
				}
			}
			scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute()
					.actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}

		// write to file
		Map<String, Integer> sortedTermCounts = this.sortByValue(totalTermCounts);
		String fileName = "D:/statistic.txt";
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
		}
		try {
			file.createNewFile();
		} catch (IOException e2) {
			e2.printStackTrace();
		}

		FileWriter fw;
		BufferedWriter bw;
		try {
			fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);

			for (String term : sortedTermCounts.keySet()) {
				bw.write(term + " " + sortedTermCounts.get(term) + "\n");
			}

			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
	    return map.entrySet()
	              .stream()
	              .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
	              .collect(Collectors.toMap(
	                Map.Entry::getKey, 
	                Map.Entry::getValue, 
	                (e1, e2) -> e1, 
	                LinkedHashMap::new
	              ));
	}
}
