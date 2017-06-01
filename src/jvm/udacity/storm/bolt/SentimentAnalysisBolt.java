package udacity.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.util.Map;
import java.util.Properties;

/**
 * Created by vinay on 1/4/17.
 */
public class SentimentAnalysisBolt extends BaseRichBolt {
    OutputCollector collector;
    public SentimentAnalysisBolt() {
        super();
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String componentId = tuple.getSourceComponent();
        String line="";
        if(componentId.equals("keyword-spout") || componentId.equals("facebook-spout"))
        {
            line=tuple.getString(1);
        }else if(componentId.equals("sandp-spout")){
            line=tuple.getString(0);
        }
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        int mainSentiment = 0;
        if (line != null && line.length() > 0) {
            int longest = 0;
            Annotation annotation = pipeline.process(line);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }

        }
        if(componentId.equals("keyword-spout") || componentId.equals("facebook-spout")) {
            String companyname=tuple.getString(0);
            collector.emit(new Values(companyname,mainSentiment));
        }else if(componentId.equals("sandp-spout")){
            String companyname="SandP";
            collector.emit(new Values(companyname,mainSentiment));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("companyname","sentiment"));
    }
}
