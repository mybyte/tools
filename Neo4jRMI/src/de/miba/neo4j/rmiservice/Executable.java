package de.miba.neo4j.rmiservice;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;

public class Executable {

	/**
	 * @param args
	 * @throws MalformedURLException
	 * @throws RemoteException
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws RemoteException,
			MalformedURLException {
		HashMap<String, String> settings = new HashMap<>();
		settings.put("org.neo4j.server.database.mode", "HA");

		GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
				.newHighlyAvailableDatabaseBuilder("db.local")
				.loadPropertiesFromFile("neo4j.properties").setConfig(settings)
				.newGraphDatabase();

		System.out.println("DB started");

		Queue<String> queryQueue = new LinkedBlockingQueue<String>();

		File failoverFile = new File("failover.ser");
		if (failoverFile.exists() && failoverFile.canRead()) {
			System.out.println("Found failover file, restoring queue...");
			try {
				ObjectInputStream ois = new ObjectInputStream(
						new FileInputStream(failoverFile));
				Object ob = ois.readObject();
				if (ob instanceof Queue<?>)
					queryQueue = (Queue<String>) ob;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {

			}
		}

		Runtime.getRuntime().addShutdownHook(
				new ShutdownActions(db, queryQueue));

		System.out.println("Shutdown hook registered");

		RemoteService service = new RemoteService(queryQueue);

		Registry registry = LocateRegistry.createRegistry(666);
		registry.rebind("QueueService", service);

		ExecutionEngine cypherEngine = new ExecutionEngine(db);

		System.out.println("Remote service bound. Starting event queue.");

		Transaction tx = db.beginTx();
		long starttime = System.currentTimeMillis();
		long timetracker = starttime;
		
		PerformanceTracker tracker = new PerformanceTracker();
		
		for(int i=0; i<3; i++){
			QueueProcessor processor = new QueueProcessor(cypherEngine, queryQueue, tracker);
			processor.start();
		}

		// event queue
		while (true) {
			long time_passed = System.currentTimeMillis() - timetracker;
			System.out.println(tracker.processedItems + " items processed. Speed: "+tracker.speed+" items/s");
			// batch commit
			if (tracker.getItemsProcessed()>0 && (tracker.getItemsProcessed() % 500 == 0 || time_passed >= 15000)) {
				synchronized (cypherEngine) {
					System.out.println("Commit.");
					tx.success();
					tx.finish();
					tx = db.beginTx();
				}
				
				timetracker = System.currentTimeMillis();				
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	

	
}
