package udacity.storm.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by vinay on 1/5/17.
 */
public class AggregatorBolt extends BaseRichBolt {
    OutputCollector collector;
    HashMap<String,Double> stockprice;
    HashMap<String,Integer> tweetsentiment;
    HashMap<String,Integer> tweetcount;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;
        this.stockprice = new HashMap<String, Double>();
        this.tweetcount= new HashMap<String, Integer>();
        this.tweetsentiment= new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple) {
        if(AggregatorBolt.isTickTuple(tuple)){
            Iterator it = stockprice.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry pair = (Map.Entry)it.next();
                String companyname=(String)pair.getKey();
                Double stockvalue= (Double)pair.getValue();
                Double avgsentiment=2.0;
                if(!tweetsentiment.containsKey(companyname)){
                    Integer sentiment = tweetsentiment.get(companyname);
                    Integer sentimenttcount=tweetcount.get(companyname);
                    try {
                        avgsentiment = (Double) ((sentiment * 1.0) / (sentimenttcount * 1.0));
                    }catch (Exception e){

                    }
                }
                collector.emit(new Values(companyname,stockvalue,avgsentiment));
                it.remove();
            }
            tweetsentiment.clear();
            tweetcount.clear();

        }else{
            String componentId = tuple.getSourceComponent();
            if(componentId.equals("stockvalue-spout")) {
                String str=tuple.getString(0);
                Double price= tuple.getDouble(1);
                if(!stockprice.containsKey(str)){
                    stockprice.put(str, price);
                }

            }else if(componentId.equals("sentiment-bolt")){
                String str=tuple.getString(0);
                Integer sentiment= tuple.getInteger(1);
                if(!tweetsentiment.containsKey(str)){
                    tweetsentiment.put(str, sentiment);
                    tweetcount.put(str,1);
                }else{

                    Integer c = tweetsentiment.get(str) + sentiment;
                    tweetsentiment.put(str, c);
                    Integer d= tweetcount.get(str)+1;
                    tweetcount.put(str,d);
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("companyname","stockvalue","sentiment"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        return conf;
    }
    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}
