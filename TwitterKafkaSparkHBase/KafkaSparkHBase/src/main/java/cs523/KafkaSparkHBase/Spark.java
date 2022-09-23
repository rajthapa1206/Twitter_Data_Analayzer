package cs523.KafkaSparkHBase;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Spark {
	
	static int counter = 1;
	private JavaSparkContext sc;
	private HBDBManager db;
	private HashMap<String, String> mapKeywords;
	
	public Spark() throws IOException {
		this.sc = new JavaSparkContext("local[*]","MySpark",new SparkConf());
		this.db = new HBDBManager();
		this.mapKeywords = this.db.GetKeyWords();
	}
	
	public void Process(String key, List<String> l) throws IOException {
		JavaRDD <String> list = this.sc.parallelize(l).flatMap(line -> Arrays.asList(line.toUpperCase().split("RT"))).filter(line -> !line.isEmpty());
		JavaPairRDD <String, Tweets>  tweetResults = list.mapToPair(new MyPairFunction(key, this.mapKeywords)).sortByKey();
		this.db.WriteTweetAnalysis(key, tweetResults.values());
	}
	
	static class MyPairFunction implements PairFunction<String, String, Tweets>,Serializable
	{
		private String key;
		private HashMap <String, String> map;
		private static final long serialVersionUID = 1L;
		public MyPairFunction(String k, HashMap <String, String> m) {
			this.key = k;
			this.map = m;
		}

		@Override
		public Tuple2<String, Tweets> call(String _line) throws Exception {
			String line = _line.toLowerCase();
			Tweets tweet = new Tweets();
			if(line.contains("@") && line.contains(":")) {
				int pos1 = line.indexOf("@");
				int pos2 = line.indexOf(":");
				tweet.user = line.substring(pos1, pos2);
			}
			for(String x : this.map.keySet()) {
				for(String s : this.map.get(x).split(",")) {
					if(line.contains(s.toLowerCase())) {
						tweet.keyword.add(new Tuple2 <String, String> (x, s));
					}
				}
			}
			if(tweet.keyword.isEmpty()) {
				tweet.keyword.add(new Tuple2 <String, String> ("General",""));
			}
			return new Tuple2 <String, Tweets> (this.key, tweet);
		}
	}
}