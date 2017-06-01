package udacity.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

/**
 * Created by vinay on 1/2/17.
 */
public class YahooSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private boolean completed = false;
    private TopologyContext context;
    String[] companysymbols={"AMZN","MSFT","AAPL","FB","GOOG","^GSPC"};
    String[] companynames={"Amazon","Microsoft","Apple","Facebook","Google","SandP"};
    int spoutindex;

    @Override

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.context = topologyContext;
        this.collector = spoutOutputCollector;
        spoutindex=topologyContext.getThisTaskIndex();
    }

    @Override
    public void nextTuple() {
        Stock stock = null;
        try {
            stock = YahooFinance.get(companysymbols[spoutindex]);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BigDecimal price = stock.getQuote().getPrice();

        this.collector.emit(new Values(companynames[spoutindex], price.doubleValue()));
        try {
            long waittime=60*1000;
            Thread.sleep(waittime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("companyname", "price"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
