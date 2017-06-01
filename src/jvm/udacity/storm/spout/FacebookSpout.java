package udacity.storm.spout;

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.Version;
import com.restfb.types.Comment;
import com.restfb.types.Post;
import java.util.regex.Pattern;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.ScheduledFuture;
//import java.util.concurrent.TimeUnit;

@SuppressWarnings("serial")
public class FacebookSpout extends BaseRichSpout
{

	String accessToken = null;
	SpoutOutputCollector _collector = null;
	LinkedBlockingQueue<String> queue = null;
	Date oneWeekAgo = null;
	Date fiveMinAgo = null;
	String[]sources;
	String[] companynames;
	//TODO make getNewComments() run independetly of nextTuple method
	//private final ScheduledExecutorService scheduler =  Executors.newScheduledThreadPool(1);;
	
	public FacebookSpout(String accessToken)
	 {
	 	try
	 	{
		 this.accessToken = accessToken;
		 sources = new String[]{"bloombergmarkets","nytimes","financialtimes","theguardian","time","washingtonpost","FoxNews","cnn","yahoonews","aljazeera","Reuters","cnnmoney","fortunebrainstormtech","wsj","marketwatch","investorsbusinessdaily","NYSE","BuzzFeedNews","cnninternational","usatoday","bloombergview","bloomberglp","latimes","Timesnow","TimesofIndia","Amazon","Google","Microsoft","facebook"};
		 companynames=new String[]{"Amazon AMZN","Microsoft MSFT","Apple AAPL","Google GOOG","Facebook FB"};
		 oneWeekAgo = new Date(System.currentTimeMillis() - 1000L * 60L * 60L * 24L * 2L);
		 fiveMinAgo = new Date(System.currentTimeMillis() - 5 * 60*1000);
		}
		catch(Exception e)
		{

			e.printStackTrace();
		}
		 
	 }

	
		

	@Override
	public void nextTuple()
	{
		if(queue.size() ==0)
		{
			getNewComments();
		}
	 	String ret = queue.poll();

				
		for(int i=0;i<companynames.length;i++)
		{
			String[]companydetail=companynames[i].split(" ");
			if(Pattern.compile(Pattern.quote(companydetail[0]), Pattern.CASE_INSENSITIVE).matcher(ret).find()){
				System.out.println("FB COMMENT BEGIN "+ ret +" FB COMMENT END");
				_collector.emit(new Values(companydetail[0],ret));
			}
			if(Pattern.compile(Pattern.quote(companydetail[1]), Pattern.CASE_INSENSITIVE).matcher(ret).find()){
				System.out.println("FB COMMENT BEGIN "+ ret +"FB COMMENT END");
				_collector.emit(new Values(companydetail[0],ret));
			}
			
		}

		try
		{
            long waittime=1000;
            Thread.sleep(waittime);
        } catch (InterruptedException e) 
        {
            e.printStackTrace();
        }
	}

	/*
	public void crawlFacebook() 
	{
	     final Runnable beeper = new Runnable()
	     {
	       public void run() { getNewComments(); }
	     };
	     
	     final ScheduledFuture<?> beeperHandle = scheduler.scheduleAtFixedRate(beeper, 10, 10, TimeUnit.SECONDS);
	     
	     scheduler.schedule(new Runnable()
	     {
	       public void run() 
	       {
	    	   beeperHandle.cancel(true); 
	       }
	     }, 60 * 60, TimeUnit.SECONDS);
	}*/
	
	public void getNewComments()
	{
		
		FacebookClient facebookClient23 = new DefaultFacebookClient(this.accessToken, Version.VERSION_2_9);
		for (String source: sources)
		 {
		 	 
			 Connection<Post> page = facebookClient23.fetchConnection(source+"/feed", Post.class,  Parameter.with("since", oneWeekAgo));
			 List<Comment> commentList;
			 do
			 {
				 List<Post> pagePosts = page.getData();
			 
			 	for(int i=0;i<pagePosts.size();i++)
			 	{
				 
			 		Post p=pagePosts.get(i);
			 		//System.out.println(p.getCreatedTime());
			 		
			 		Connection<Comment> comments = facebookClient23.fetchConnection(p.getId()+"/comments", Comment.class,Parameter.with("since", fiveMinAgo));
			 		//System.out.println(comments.getData().size());
			 		List<Comment> commentData = comments.getData();
			 		if(p.getCreatedTime().compareTo(fiveMinAgo) > 1)
			 		{
			 			queue.offer(p.getMessage());
			 		}
			 		for(Comment comment: commentData)
			 		{
			 			queue.offer(comment.getMessage());
			 		}	 
			 	}
			 	if(page.hasNext())
			 	{
			 		page = facebookClient23.fetchConnectionPage(page.getNextPageUrl(),Post.class);
			 	}
			 	else
			 	{
			 		break;
			 	}
			 }
			 while(true);
		 }
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) 
	{
		queue = new LinkedBlockingQueue<String>(1000);
		_collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("companyname","comment"));
	}
}
