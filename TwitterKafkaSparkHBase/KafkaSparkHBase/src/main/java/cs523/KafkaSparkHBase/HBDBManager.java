package cs523.KafkaSparkHBase;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;

public class HBDBManager {
	
	private Configuration hbConfig;
	private int rowKeyAnalysis = 0;
	int count = 0;
	
	public HBDBManager() throws IOException {
		this.hbConfig = HBaseConfiguration.create();
		this.DefaultValues();
		this.rowKeyAnalysis = this.GetMaxRowNum();
	}
	
	private void DefaultValues() throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(this.hbConfig);
				Admin admin = connection.getAdmin())	{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TableDetails.KEYWORDS_TABLE));
			table.addFamily(new HColumnDescriptor(TableDetails.FAMILY_TYPE).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(TableDetails.FAMILY_KEYWORDS));
			if (admin.tableExists(table.getTableName()))
			{
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			if (!admin.tableExists(table.getTableName()))
			{
				admin.createTable(table);		
				Table tbl = connection.getTable(TableName.valueOf(TableDetails.KEYWORDS_TABLE));
				
				Put put1 = new Put(Bytes.toBytes("1"));
				put1.addColumn(Bytes.toBytes(TableDetails.FAMILY_TYPE),Bytes.toBytes("type"),Bytes.toBytes(TableDetails.COVID));
				put1.addColumn(Bytes.toBytes(TableDetails.FAMILY_KEYWORDS),Bytes.toBytes("keywords"),Bytes.toBytes(TableDetails.COVID));
				tbl.put(put1);
				
				Put put2 = new Put(Bytes.toBytes("2"));
				put2.addColumn(Bytes.toBytes(TableDetails.FAMILY_TYPE),Bytes.toBytes("type"),Bytes.toBytes(TableDetails.MADRID));
				put2.addColumn(Bytes.toBytes(TableDetails.FAMILY_KEYWORDS),Bytes.toBytes("keywords"),Bytes.toBytes(TableDetails.MADRID));
				tbl.put(put2);
				
				Put put3 = new Put(Bytes.toBytes("3"));
				put3.addColumn(Bytes.toBytes(TableDetails.FAMILY_TYPE),Bytes.toBytes("type"),Bytes.toBytes(TableDetails.ELON));
				put3.addColumn(Bytes.toBytes(TableDetails.FAMILY_KEYWORDS),Bytes.toBytes("keywords"),Bytes.toBytes(TableDetails.ELON));
				tbl.put(put3);
				
				Put put4 = new Put(Bytes.toBytes("4"));
				put4.addColumn(Bytes.toBytes(TableDetails.FAMILY_TYPE),Bytes.toBytes("type"),Bytes.toBytes(TableDetails.RONALDO));
				put4.addColumn(Bytes.toBytes(TableDetails.FAMILY_KEYWORDS),Bytes.toBytes("keywords"),Bytes.toBytes(TableDetails.RONALDO));
				tbl.put(put4);
				
				Put put5 = new Put(Bytes.toBytes("5"));
				put5.addColumn(Bytes.toBytes(TableDetails.FAMILY_TYPE),Bytes.toBytes("type"),Bytes.toBytes(TableDetails.UNITED));
				put5.addColumn(Bytes.toBytes(TableDetails.FAMILY_KEYWORDS),Bytes.toBytes("keywords"),Bytes.toBytes(TableDetails.UNITED));
				tbl.put(put5);
		
				tbl.close();
			}
		}		
	}
	
	public HashMap<String,String> GetKeyWords() throws IOException
	{
		HashMap<String,String> map = new HashMap<String,String>();
		try (Connection connection = ConnectionFactory.createConnection(this.hbConfig))
		{
			Table tbl = connection.getTable(TableName.valueOf(TableDetails.KEYWORDS_TABLE));
			Scan scan = new Scan();
			scan.setCacheBlocks(false);
			scan.setCaching(10000);
			scan.setMaxVersions(10);
			ResultScanner scanner = tbl.getScanner(scan);
			for (Result result = scanner.next(); result != null; result = scanner.next()) 
			{
				String type = "";
				String keywords = "";
				for (Cell cell : result.rawCells()) 
				{
					String family = Bytes.toString(CellUtil.cloneFamily(cell));
					String column = Bytes.toString(CellUtil.cloneQualifier(cell));
					if(family.equalsIgnoreCase(TableDetails.FAMILY_TYPE) && column.equalsIgnoreCase("type"))
					{
						type = Bytes.toString(CellUtil.cloneValue(cell));
					}
					else if(family.equalsIgnoreCase(TableDetails.FAMILY_KEYWORDS) && column.equalsIgnoreCase("keywords"))
					{
						keywords = Bytes.toString(CellUtil.cloneValue(cell));
					}
				}		
				if(!map.containsKey(type))
				{
					map.put(type, keywords);
				}
				else
				{
					map.replace(type, map.get(type) + "," + keywords);
				}
			}
		}	
		return map;
	}

	public void WriteTweetAnalysis(String key, JavaRDD <Tweets> rdd) throws IOException
	{
		
		try (Connection connection = ConnectionFactory.createConnection(this.hbConfig);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TableDetails.TWEET_ANALYSIS_TABLE));
			table.addFamily(new HColumnDescriptor(TableDetails.FAMILY_KEY).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(TableDetails.FAMILY_TWEET));
			if (!admin.tableExists(table.getTableName()))
			{
				admin.createTable(table);
			}
		
			Table tbl = connection.getTable(TableName.valueOf(TableDetails.TWEET_ANALYSIS_TABLE));
			for(Tweets tw : rdd.collect())
			{
				Put put = new Put(Bytes.toBytes(String.valueOf(++this.rowKeyAnalysis)));
				put.addColumn(Bytes.toBytes(TableDetails.FAMILY_KEY),Bytes.toBytes(TableDetails.KEY),Bytes.toBytes(key));
				put.addColumn(Bytes.toBytes(TableDetails.FAMILY_KEY),Bytes.toBytes(TableDetails.USER),Bytes.toBytes(tw.user));
				put.addColumn(Bytes.toBytes(TableDetails.FAMILY_TWEET),Bytes.toBytes(TableDetails.TWEET_ANALYSIS),Bytes.toBytes(tw.getStatement()));
				put.addColumn(Bytes.toBytes(TableDetails.FAMILY_TWEET),Bytes.toBytes(TableDetails.KEYWORD),Bytes.toBytes(tw.getFoundKeyWords()));	
				tbl.put(put);
				count++;
			}
			tbl.close();
			
			System.out.println("tweet_analysis_tbl written rows count:" + count);
		}
	}
	
	@SuppressWarnings({ "finally", "deprecation" })
	private int GetMaxRowNum()
	{
		try {
			@SuppressWarnings("resource")
			Result result = new HTable(this.hbConfig,TableDetails.TWEET_ANALYSIS_TABLE).getRowOrBefore(Bytes.toBytes("9999"),Bytes.toBytes(""));
			return Integer.parseInt(Bytes.toString(result.getRow()));
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		finally {
			return 0;
		}
	}
	
}