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
 * Created by vinay on 1/4/17.
 */
public class RedisPublishbolt extends BaseRichBolt {
    transient RedisConnection<String,String> redis;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        RedisClient client = new RedisClient("localhost",6379);
        redis = client.connect();
    }

    @Override
    public void execute(Tuple tuple) {
        String company = tuple.getString(0);
        String tweet = tuple.getString(1);
        redis.publish("companywordcloud", company + "|" + (tweet));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
