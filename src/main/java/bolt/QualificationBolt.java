package bolt;

import java.util.Map;

import org.apache.log4j.Logger;

import sqlite.SQLiteJDBC;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
public class QualificationBolt extends BaseBasicBolt {
	private final static Logger LOG = Logger.getLogger(QualificationBolt.class);
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String userid = input.getStringByField("userid");
		Integer currentPV = input.getIntegerByField("currentPV");

		int learnedBI = getCookedBI();

		if ( currentPV >= learnedBI) {
			LOG.info("Heavy user : " + userid);
		}
		LOG.info(userid + " : " + currentPV);
	}

	private int getCookedBI() {
		//
		// Cook Buying Intension Here
		//
		return 3;
	}
	
	@Override
	public void cleanup() {

	}

	/**
	 * On create
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	

}
