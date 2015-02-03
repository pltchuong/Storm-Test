package com.detica.cyberreveal.storm.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * A storm spout which reads a file and outputs each line to a spearate tuple.
 */
public class BookLineSpout extends BaseRichSpout {

	private static final long serialVersionUID = -7281111950770566776L;
	private SpoutOutputCollector collector;
	private File inputFile;
	private boolean completed = false;

	@Override
	public void open(@SuppressWarnings("rawtypes") final Map conf,
			final TopologyContext context,
			final SpoutOutputCollector spoutCollector) {
		this.collector = spoutCollector;
		this.inputFile = new File((String) conf.get("inputFile"));
	}

	@Override
	public void nextTuple() {
		if(completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				
			}
			return;
		}
		
		try {
			FileReader inStream = new FileReader(inputFile);
			try {
				BufferedReader buff = new BufferedReader(inStream);
				try {
					String line = null;
					while ((line = buff.readLine()) != null) {
						this.collector.emit(new Values(line));
					}
				} finally {
					buff.close();
				}
			} finally {
				inStream.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			completed = true;
		}
	}

	@Override
	public void ack(final Object id) {
		System.err.println("acknowledged: " + id.toString());
	}

	@Override
	public void fail(final Object id) {
		System.err.println("failed: " + id.toString());
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
