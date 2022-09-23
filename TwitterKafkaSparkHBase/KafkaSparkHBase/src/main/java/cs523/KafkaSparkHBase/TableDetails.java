package cs523.KafkaSparkHBase;

public abstract class TableDetails {
	
	// Region KEYWORD TABLE
	
	protected static String KEYWORDS_TABLE = "keywords_tbl";
	protected static String FAMILY_TYPE = "fam_type";
	protected static String FAMILY_KEYWORDS = "fam_keywords";
	
	//Column
	
	protected static String COVID = "Covid19";
	protected static String MADRID = "madrid";
	protected static String RONALDO = "ronaldo";
	protected static String ELON = "elon";
	protected static String UNITED = "united";
		
	//End Column
	
	// End Region
	
	// Region TWEET ANALYSIS TABLE
	
	protected static String TWEET_ANALYSIS_TABLE = "tweet_analysis_tbl";
	protected static String FAMILY_KEY = "fam_key";
	protected static String FAMILY_TWEET = "fam_tweet";
	
	//Column
	
	protected static String KEY = "key";
	protected static String USER = "user";
	protected static String TWEET_ANALYSIS = "tweet_analysis";
	protected static String KEYWORD = "keyword";
	
	//End Column
	
	// End Region
	
}