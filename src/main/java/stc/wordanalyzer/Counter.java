package stc.wordanalyzer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

public class Counter {

  private final String index = "nutch";
  private final String type = "doc";
  private static final String stopwords_1[] = {"home", "contact", "image", "about", "navigation", "news", "event",
      "copyright", "registration", "link", "search", "also", "contribution", "help", "page", "logo", "citation", 
      "wiki", "site map", "menu", "community", "media", "wiki", "alumni", "http", "www", "htm", "blog", "facebook",
      "twitter", "youtube", "terms of use", "subscribe", "become", "education", "cookie", "google", "career", "vimeo",
      "mail", "sign", "upload", "press", "featured", "skip", "404"};

  private static final String stopwords_2[] = {"en", "org", "php", "com", "rss", "gov", "doc", "asp", "tag", "people", "faq", "overview", "edu",
      "null", "index", "instagram", "km", "yr", "net", "login", "log in", "forum", "kg", "staff", "video", "get involved",
      "connect", "talk", "jobs", "ajax", "travel", "privacy policy", "talk", "donate", "volunteer", "explore", "categories",
      "the team", "advertise", "recent changes", "rss feed", "publications", "glossary", "xml", "history", "english", "resources",
  "discussion"};

  private String include_self = "1"; // need to be implemeted
  private String include_in = "1"; // need to be implemeted
  private String include_out = "1"; // need to be implemeted

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

    if(args.length<3)
    {
      System.out.println("Please input the output directory, score threshold, and round number!");
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
      counter.include_self = args[3];
      counter.include_in = args[4];
      counter.include_out = args[5];

      counter.sumTermsForAllNRound(client, args[0], Double.parseDouble(args[1]), Integer.parseInt(args[2]));
    }

  }

  public void sumTermsForAllNRound(Client client, String outDir, double T, int roundNum)
  {
    List<String> segList = getSegList(client);
    List<String> output_segList = new ArrayList<>();
    int i = 0;
    for(String seg:segList)
    {
      if(i>roundNum)
        break;
      String outpath = outDir + seg + ".txt";
      sumTermsForEachRound(client, seg, outpath, T, true, null);
      output_segList.add(seg);
      i++;
    }

    //produce the aggregated file
    sumTermsForFNRound(client, output_segList, outDir + "agg.txt", T);
  }

  public List<String> getSegList(Client client)
  {
    SearchResponse sr = client.prepareSearch(index)
        .setTypes(type).setQuery(QueryBuilders.matchAllQuery())
        .setSize(0).addAggregation(
            AggregationBuilders.terms("segs").field("segment").size(0))
        .execute().actionGet();
    Terms segs = sr.getAggregations().get("segs");
    List<String> segList = new ArrayList<>();
    for (Terms.Bucket entry : segs.getBuckets()) {
      segList.add(entry.getKey().toString());
    }

    return segList;    
  }

  public void sumTermsForFNRound(Client client, List<String> seglist, String outpath, double T)
  {
    Map<String, Integer> aggCounts = new HashMap<String, Integer>();  
    for(String seg:seglist)
    {
      aggCounts = sumTermsForEachRound(client, seg, null, T, false, aggCounts);
    }
    Map<String, Integer> sortedCounts = Counter.sortByValue(aggCounts);
    writeToFile(sortedCounts, outpath);
  }

  public Map<String, Integer> sumTermsForEachRound(Client client, String seg, String outpath, double T, boolean output, 
      Map<String, Integer> preMap) {
    Map<String, Integer> totalCounts;
    if(preMap == null)
      totalCounts = new HashMap<String, Integer>();
    else
      totalCounts = preMap;

    QueryBuilder filterSearch = QueryBuilders.matchAllQuery();
    if(seg!=null)
      filterSearch = QueryBuilders.termQuery("segment", seg);

    SearchResponse scrollResp = client.prepareSearch(index).setTypes(type).setQuery(filterSearch)
        .setFetchSource(null, new String[]{"content"})
        .setScroll(new TimeValue(60000)).setSize(100).execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();

        String lang = (String) result.get("lang");

        if(lang==null)
          continue;
        if(!lang.equals("en"))
          continue;

        Set<String> pageTerms = new HashSet<String>();
        String inlinks = "";
        String outlinks = "";
        String title = "";
        String inurls = "";
        String outurls = "";
        String url = "";

        //page title
        if(include_self.equals("1"))
        {
          title = (String) result.get("title");
          if(title!=null)
          {
            title = title.replace("|", "&&");
            for(int w =0; w<3; w++)
            {
              title = title + "&&";
            }
          }

          url = (String) result.get("url");
          url = url.replace("/", "&&").replace(".", "&&");
          for(int w =0; w<2; w++)
          {
            url = url + "&&";
          }

        }

        if(include_in.equals("1"))
        {
          inurls = (String) result.get("inlinks");
          if(inurls!=null)
            inurls = inurls.replace("/", "&&").replace(".", "&&");

          inlinks = (String) result.get("anchor_inlinks");
        }

        //various out text
        if(include_out.equals("1"))
        {         
          outlinks = (String) result.get("anchor_outlinks");
          outurls = (String) result.get("outlinks");
          if(outurls!=null)
            outurls = outurls.replace("/", "&&").replace(".", "&&");
        }

        double nutch_score = (double) result.get("nutch_score");

        if(nutch_score<T)   //similarity threshold
          continue;

        String text = inlinks + "&&" + outlinks + "&&" + title + "&&" + inurls + "&&" + outurls + "&&" + url;
        String[] linksTemrs = text.split("&&");
        for (String term : linksTemrs) {
          term = term.toLowerCase().replaceAll("\\W", " ").replace("_", " ").trim();
          if(!term.equals("") && term.matches(".*[a-zA-Z]+.*") && !isStopwords(term) && term.length()>1){
            if(!Character.isDigit(term.charAt(0)))
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

    if(output)
    {
      Map<String, Integer> sortedCounts = Counter.sortByValue(totalCounts);
      writeToFile(sortedCounts, outpath);
    }

    return totalCounts;

  }

  public void writeToFile(Map<String, Integer> map, String outpath)
  {
    // write and save to file    
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

      for (String term : map.keySet()) {
        bw.write(term + " " + map.get(term) + "\n");
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

  public static boolean isStopwords(String str)
  {
    for(String s: stopwords_1)
    {
      if(str.contains(s))
        return true;
    }

    for(String s: stopwords_2)
    {
      if(str.equals(s))
        return true;
    }
    return false;
  }
}
