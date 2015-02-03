package com.detica.cyberreveal.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LetterSplitBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -1832695156509104292L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getStringByField("line");
		String[] letters = line.toLowerCase().replaceAll("\\W", "").split("|");
		for (String letter : letters) {
			collector.emit(new Values(letter));
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("letter"));
	}

}
