package udacity.storm;

/**
 * Created by vinay on 12/31/16.
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;

import backtype.storm.topology.TopologyBuilder;

import backtype.storm.tuple.Fields;
import udacity.storm.bolt.*;
import udacity.storm.spout.*;


import java.io.*;
import java.util.ArrayList;

public class Twittertopology
 {
    public static void main(String[] args) throws InterruptedException, IOException {
        String consumerKey = "0Iek0jQSJuXOPM8nDZQh2jFn2";
        String consumerSecret = "donf2xxlxtjEfYWj6XkeULe11PSugp88pnGXFLVFfBOaSgajeD";
        String accessToken = "814393855223025664-NNwJBBQbMika62Uwj7ZPpAynzJEJXeC";
        String accessTokenSecret = "S9OdILjA7omtzDSbaAc7LrNHaCTUlQZR94A0zyRxmgdjT";
        Twittertopology twittertopology= new Twittertopology();
        ClassLoader classLoader=twittertopology.getClass().getClassLoader();
        File file = new File("Keywords.txt");
        BufferedReader bufferedReader=new BufferedReader(new FileReader(file));
        ArrayList<String> list=new ArrayList<String>();
        String line="";
        while ((line=bufferedReader.readLine())!=null){
            list.add(line);
        }
		String facebookAccessToken = "EAACEdEose0cBAOnGm22nLK3KVzOIOLXhShIebmjSk9X0lo2UbosdZBQXrkUqLyTiKdCOnkY5z1E9DNCZClbRwNlj0BR4V3kB1LJJsZCZB6IvCoJT50hVRZALlQPp5WVjyzkYrNAER5TLG9N7TGRleZBbiQusBdqYN6mRrZCRJnZAjMvsJ2GhJxpNZCJdAlBnflSAZD";
        String[]keywords_topcompanies=list.toArray(new String[0]);
        list.clear();
        file= new File("Stocks.txt");
        bufferedReader=new BufferedReader(new FileReader(file));
        while ((line=bufferedReader.readLine())!=null){
            list.add(line);
        }

        String[] keyWords_SandP=list.toArray(new String[0]);


        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("keyword-spout", new TweetSpoutStock(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keywords_topcompanies),1);
        builder.setSpout("sandp-spout", new TweetSpoutSandP(consumerKey,
                        consumerSecret, accessToken, accessTokenSecret, keyWords_SandP),1);
		builder.setSpout("facebook-spout", new FacebookSpout(facebookAccessToken),1);
        builder.setSpout("stockvalue-spout", new YahooSpout(),6);
        builder.setBolt("sentiment-bolt", new SentimentAnalysisBolt(), 8).shuffleGrouping("sandp-spout").shuffleGrouping("keyword-spout").shuffleGrouping("facebook-spout");
        builder.setBolt("parsetweet-bolt", new ParseTweetBolt(), 8).shuffleGrouping("sandp-spout").shuffleGrouping("keyword-spout").shuffleGrouping("facebook-spout");

        builder.setBolt("redispublishword-bolt", new RedisPublishbolt(),1).globalGrouping("parsetweet-bolt");
        builder.setBolt("aggregator-bolt",new AggregatorBolt(),3).fieldsGrouping("sentiment-bolt",new Fields("companyname")).fieldsGrouping("stockvalue-spout",new Fields("companyname"));
        builder.setBolt("predictor-bolt",new LinearRegress(),1).globalGrouping("aggregator-bolt");
       builder.setBolt("reportprediction-bolt", new ReportPrediction(),1).globalGrouping("predictor-bolt");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config,builder.createTopology());
        //Thread.sleep(100000);
        //cluster.shutdown();
    }
}
