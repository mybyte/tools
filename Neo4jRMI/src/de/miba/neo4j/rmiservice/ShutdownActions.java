package de.miba.neo4j.rmiservice;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Queue;

import org.neo4j.graphdb.GraphDatabaseService;

public class ShutdownActions extends Thread {
	
	private GraphDatabaseService db;
	private Queue<String> queries;

	public ShutdownActions(GraphDatabaseService db, Queue<String> queries){
		this.db = db;
		this.queries = queries;
	}
	
	@Override
	public void run() {
		System.out.println("Running hook");
		// Shutdown the database
		db.shutdown();
		
		// persist queries
		synchronized (queries) {
			if(queries.size()>0){
				try {
					ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("failover.ser")));
					oos.writeObject(queries);
					oos.flush();
					oos.close();
					
					queries.clear();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
		}
	}

}
