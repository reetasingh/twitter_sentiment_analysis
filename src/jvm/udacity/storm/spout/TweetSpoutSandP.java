package udacity.storm.spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by vinay on 12/28/16.
 */
public class TweetSpoutSandP extends BaseRichSpout{
    SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream _twitterStream;
    String consumerKey;
    String consumerSecret;
    String accessToken;
    String accessTokenSecret;
    String[] keyWords;

    public TweetSpoutSandP(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret, String[] keyWords) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.keyWords = keyWords;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        queue=new LinkedBlockingQueue<Status>(1000);

        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {

                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onException(Exception ex) {
            }

            @Override
            public void onStallWarning(StallWarning arg0) {
                // TODO Auto-generated method stub

            }

        };
        TwitterStream twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();

        twitterStream.addListener(listener);
        twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        twitterStream.setOAuthAccessToken(token);

        if (keyWords.length == 0) {

            twitterStream.sample();
        }

        else {

            FilterQuery query = new FilterQuery().track(keyWords);
            String[] lang = { "en" };
            query.language(lang);
            twitterStream.filter(query);

        }
    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();

        if (ret == null) {
            Utils.sleep(50);
        } else {
            System.out.println(" TWEET BEGIN "+ ret.getText()+" TWEET END");

            _collector.emit(new Values(ret.getText()));

        }
    }
    @Override
    public void close() {
        _twitterStream.shutdown();
    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }
}
