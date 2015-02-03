package com.detica.cyberreveal.storm.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LetterCountBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -1832695156509104292L;
	private Map<String, Long> letterCounts = new HashMap<String, Long>();
		
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String letter = input.getStringByField("letter");
		Long letterCount = letterCounts.get(letter);
		if(letterCount == null) {
			letterCount = 0L;
		}
		letterCount++;
		letterCounts.put(letter, letterCount);
		collector.emit(new Values(letter, letterCount));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("letter", "count"));
	}

	@Override
	public void cleanup() {
		try {
			FileWriter writer = new FileWriter("target/letterOutput.txt", true);
			for (Entry<String, Long> entry : letterCounts.entrySet()) {
				writer.append(entry.getKey() + ": " + entry.getValue() + "\n");
			}
			writer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
