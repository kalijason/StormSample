package topology;

import spout.UserLogSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolt.LearningBolt;
import bolt.QualificationBolt;

public class TopologyMain {
	
	public static void main(String[] args) throws InterruptedException {

		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("logReader", new UserLogSpout());

		//builder.setBolt("learningBolt", new LearningBolt(),2).shuffleGrouping("logReader");

		builder.setBolt("learningBolt", new LearningBolt(), 2).fieldsGrouping("logReader", new Fields("userid"));

		builder.setBolt("qualificationBolt", new QualificationBolt(), 1).shuffleGrouping("learningBolt");

		// Configuration
		Config conf = new Config();
		conf.setDebug(false);

		// Topology run
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("pinball", conf, builder.createTopology());
		Thread.sleep(1000 * 60 * 5);
		cluster.shutdown();

	}
}
