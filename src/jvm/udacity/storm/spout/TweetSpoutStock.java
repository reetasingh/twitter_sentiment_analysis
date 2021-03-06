/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package udacity.storm.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings("serial")
public class TweetSpoutStock extends BaseRichSpout {

	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[]keyWords;
	String[] companynames;
	int spoutid;
	
	public TweetSpoutStock(String consumerKey, String consumerSecret,
                           String accessToken, String accessTokenSecret, String[]keyWords) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
		companynames=new String[]{"Amazon AMZN","Microsoft MSFT","Apple AAPL","Google GOOG","Facebook FB"};


	}

	public TweetSpoutStock() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;
		int myIdx = context.getThisTaskIndex();


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
			for(int i=0;i<companynames.length;i++){
				String[]companydetail=companynames[i].split(" ");
				if(Pattern.compile(Pattern.quote(companydetail[0]), Pattern.CASE_INSENSITIVE).matcher(ret.getText()).find()){
					System.out.println(" TWEET BEGIN "+ ret.getText()+" TWEET END");
					_collector.emit(new Values(companydetail[0],ret.getText()));
				}
				if(Pattern.compile(Pattern.quote(companydetail[1]), Pattern.CASE_INSENSITIVE).matcher(ret.getText()).find()){
					System.out.println(" TWEET BEGIN "+ ret.getText()+" TWEET END");
					_collector.emit(new Values(companydetail[0],ret.getText()));
				}
			}


		}
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(5);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("companyname","tweet"));
	}

}
