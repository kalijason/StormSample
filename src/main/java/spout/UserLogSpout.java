package spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class UserLogSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private FileReader fileReader;

	@Override
	public void nextTuple() {
		try {
			this.fileReader = new FileReader("src/main/resources/dump_20140201-test.csv");
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		String str;
		// Open the reader
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			// Read all lines
			while ((str = reader.readLine()) != null) {
				/**
				 * By each line emit a new value with the line as a their
				 */
				Thread.sleep(500);
				String strAry[] = str.split(",");
				String userid = strAry[0];
				String type = strAry[1];
				String category = strAry[2];
				String productId = strAry[3];

				this.collector.emit(new Values(userid, type, category, productId));
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple", e);
		}
	}

	/**
	 * Declare the output field "word"
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("userid", "type", "category", "productId"));
	}

	/**
	 * We will create the file and get the collector object
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("OK:" + msgId);
	}

	@Override
	public void close() {
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("FAIL:" + msgId);
	}

}