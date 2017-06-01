package udacity.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by vinay on 1/4/17.
 */
public class ParseTweetBolt extends BaseRichBolt {
    OutputCollector collector;

    private String[] skipWords = {"rt", "to", "me","la","on","that","que",
            "followers","watch","know","not","have","like","I'm","new","good","do",
            "more","es","te","followers","Followers","las","you","and","de","my","is",
            "en","una","in","for","this","go","en","all","no","don't","up","are",
            "http","http:","https","https:","http://","https://","with","just","your",
            "para","want","your","you're","really","video","it's","when","they","their","much",
            "would","what","them","todo","FOLLOW","retweet","RETWEET","even","right","like",
            "bien","Like","will","Will","pero","Pero","can't","were","Can't","Were","TWITTER",
            "make","take","This","from","about","como","esta","follows","followed"};

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        String componentId = tuple.getSourceComponent();
        String companyname="";
        String tweet="";
        if(componentId.equals("keyword-spout") || componentId.equals("facebook-spout")) {
            companyname=tuple.getString(0);
            tweet=tuple.getString(1);

        }
        else if(componentId.equals("sandp-spout")){
            companyname="SandP";
            tweet=tuple.getString(0);
        }
        collector.emit(new Values(companyname,tweet));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("companyname","word"));
    }
}
