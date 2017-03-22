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

    if(args.length<1)
    {
      System.out.println("Please input the output path!");
      return;
    }

    Counter counter = new Counter();

    Client client = null;
    try {
      client = counter.makeClient();
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (client != null) {
      System.out.println("ES client created successfully.");
      counter.sumTerms(client, args[0]);
    }
  }

  public void sumTerms(Client client, String outpath) {

    Map<String, Integer> totalCounts = new HashMap<String, Integer>();

    String index = "nutch";
    String type = "doc";
    SearchResponse scrollResp = client.prepareSearch(index).setTypes(type).setQuery(QueryBuilders.matchAllQuery())
        .setFetchSource(null, new String[]{"content"})
        .setScroll(new TimeValue(60000)).setSize(100).execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();

        Set<String> pageTerms = new HashSet<String>();
        String inlinks = (String) result.get("anchor_inlinks");
        String outlinks = (String) result.get("anchor_outlinks");
        String title = (String) result.get("title");

        String text = inlinks + "&&" + outlinks + "&&" + title;
        String[] linksTemrs = text.split("&&");
        for (String term : linksTemrs) {
          term = term.trim();
          if(!term.isEmpty()){
            pageTerms.add(term);
          }
        }

        for (String term : pageTerms) {
          if (totalCounts.containsKey(term)) {
            int count = totalCounts.get(term);
            totalCounts.put(term, count + 1);
          } else {
            totalCounts.put(term, 1);
          }
        }
      }

      scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute()
          .actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    // write and save to file
    Map<String, Integer> sortedCounts = Counter.sortByValue(totalCounts);
    File file = new File(outpath);
    if (file.exists()) {
      file.delete();
    }
    try {
      file.createNewFile();
    } catch (IOException e2) {
      e2.printStackTrace();
    }

    FileWriter fw = null;
    BufferedWriter bw = null;
    try {
      fw = new FileWriter(file.getAbsoluteFile());
      bw = new BufferedWriter(fw);

      for (String term : sortedCounts.keySet()) {
        bw.write(term + " " + sortedCounts.get(term) + "\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }finally{
      try{      
        if(bw!=null)
        {
          bw.close();
        }
        
        if(fw!=null)
        {
          fw.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

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
