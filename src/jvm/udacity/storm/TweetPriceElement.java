package udacity.storm;

/**
 * Created by vinay on 1/6/17.
 */
public class TweetPriceElement {
    Double Sentiment;
    Double StockPrice;

    public Double getSentiment() {
        return Sentiment;
    }

    public void setSentiment(Double sentiment) {
        Sentiment = sentiment;
    }

    public Double getStockPrice() {
        return StockPrice;
    }

    public void setStockPrice(Double stockPrice) {
        StockPrice = stockPrice;
    }

    public TweetPriceElement(Double sentiment, Double stockPrice) {

        Sentiment = sentiment;
        StockPrice = stockPrice;
    }
}
