package stc.wordanalyzer;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;

public class Counter {

  public Counter() {
    // TODO Auto-generated constructor stub
  }

  public static void main(String[] args) {
    Settings settings = Settings.settingsBuilder().build();
    Client client = TransportClient.builder().settings(settings).build();
    
    if(client!=null)
    {
      System.out.println("you made it");
    }
  }

}
