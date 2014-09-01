package bolt;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import sqlite.SQLiteJDBC;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
public class LearningBolt extends BaseBasicBolt {
	private final static Logger LOG = Logger.getLogger(LearningBolt.class);
	private static final long serialVersionUID = 1L;
	
	
	
	LoadingCache<String, Integer> pv;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String userid = input.getStringByField("userid");
		String type = input.getStringByField("type");
		String category = input.getStringByField("category");
		String productId = input.getStringByField("productId");

		// Print
		//LOG.info("[INPUT] userid=" + userid + ",type=" + type + ",category=" + category + ",productId=" + productId);

		// Memorize User's PV

		
		Integer previousPV = pv.getUnchecked(userid);

		Integer currentPV = previousPV + 1;
		pv.put(userid, currentPV);

		LOG.info(userid + " is coming, PV:  " + currentPV);
		
		
		// Simply check
		
		//if (currentPV>=3){
		//	 LOG.info("Heavy user : " + userid);
		//}
		/*
		*/
		// emit
		 collector.emit(new Values(userid, currentPV));
	}

	/**
	 * On create
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		pv = CacheBuilder.newBuilder().softValues().build(new CacheLoader<String, Integer>() {
			@Override
			public Integer load(String key) throws Exception {
				return 0;
			}
		});
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("userid", "currentPV"));
	}

	@Override
	public void cleanup() {

	}
}