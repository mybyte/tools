package de.miba.neo4j.rmiservice;

import java.util.Queue;

import org.neo4j.cypher.javacompat.ExecutionEngine;

class QueueProcessor extends Thread {

	private Queue<String> queue;
	private PerformanceTracker tracker;
	private ExecutionEngine cypher;

	public QueueProcessor(ExecutionEngine cypher, Queue<String> queryQueue,
			PerformanceTracker tracker) {
		this.queue = queryQueue;
		this.tracker = tracker;
		this.cypher = cypher;
	}

	@Override
	public void run() {
		while (!Thread.interrupted()) {
			String query = null;
			synchronized (queue) {
				while (queue.isEmpty()) {
					try {
						queue.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				if (queue.isEmpty())
					continue;

				query = queue.remove();
			}
			if (query != null && !query.isEmpty()) {
				try {
					synchronized (cypher) {
						cypher.execute(query);
					}
					tracker.tick();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}

}
