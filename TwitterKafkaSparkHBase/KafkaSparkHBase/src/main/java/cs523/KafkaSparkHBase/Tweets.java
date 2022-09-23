package cs523.KafkaSparkHBase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import scala.Tuple2;

public class Tweets implements Serializable{
	
	private static final long serialVersionUID = 154267L;
	public String user = "";
	public List<Tuple2<String,String>> keyword;
	
	
	public Tweets() {
		this.keyword = new ArrayList<Tuple2<String,String>>(); 
	}
	
	public String getStatement() {
		if(this.keyword.isEmpty()) return "";
		return this.keyword.stream().map(t -> t._1()).collect(Collectors.joining(","));
	}
	
	public String getFoundKeyWords() {
		if(this.keyword.isEmpty()) return "";
		return this.keyword.stream().map(t -> t._2()).collect(Collectors.joining(","));
	}
	
	@Override
	public String toString() {
		return this.user + "|" + this.getStatement()+"|" + this.getFoundKeyWords();
	}
}