package udacity.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import udacity.storm.TweetPriceElement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vinay on 1/6/17.
 */
public class LinearRegress extends BaseRichBolt {
    OutputCollector collector;
    HashMap<String,ArrayList<TweetPriceElement>>data;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;
        data=new HashMap<String, ArrayList<TweetPriceElement>>();
    }

    @Override
    public void execute(Tuple tuple) {
        String companyname=tuple.getString(0);
        Double stockprice = tuple.getDouble(1);
        Double sentiment = tuple.getDouble(2);
        Double PredictedValue= -1.0;
        TweetPriceElement input= new TweetPriceElement(sentiment,stockprice);
        ArrayList<TweetPriceElement> arrayList=data.get(companyname);
        int size=0;
        if(arrayList!=null){
            size=arrayList.size();
        }
        double[][] linearregressdata= new double[size][2];
        for(int i=0;i<size;i++){
            linearregressdata[i][0]=arrayList.get(i).getSentiment();
            linearregressdata[i][1]=arrayList.get(i).getStockPrice();
        }
        SimpleRegression simpleRegression = new SimpleRegression(true);
        simpleRegression.addData(linearregressdata);
        if(Double.isNaN(simpleRegression.predict(sentiment))){
            collector.emit(new Values(companyname,stockprice,PredictedValue,sentiment));
        }else{
            PredictedValue=simpleRegression.predict(sentiment);
            collector.emit(new Values(companyname,stockprice,PredictedValue,sentiment));
        }
        if(!data.containsKey(companyname)){
            data.put(companyname,new ArrayList<TweetPriceElement>());
        }
        data.get(companyname).add(input);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("companyname","stockprice","predictedvalue","sentiment"));
    }
}
