package udacity.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import java.util.Map;

/**
 * Created by vinay on 1/7/17.
 */
public class ReportPrediction extends BaseRichBolt {
    transient RedisConnection<String,String> redis;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        RedisClient client = new RedisClient("localhost",6379);
        redis = client.connect();
    }

    @Override
    public void execute(Tuple tuple) {
        String companyname = tuple.getString(0);
        Double stockprice = tuple.getDouble(1);
        Double predictedstockprice= tuple.getDouble(2);
        Double sentiment=tuple.getDouble(3);
        redis.publish("stockgraph", companyname + "|" + stockprice+"|"+predictedstockprice+"|"+sentiment);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
