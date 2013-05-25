package de.miba.neo4j.rmiservice;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Queue;

public class RemoteService extends UnicastRemoteObject implements ServiceInterface{

	private static final long serialVersionUID = 1L;
	
	private Queue<String> queryQueue;
	
	protected RemoteService(Queue<String> queue) throws RemoteException {
		super();
		
		queryQueue = queue;
	}

	@Override
	public void addQueryToQueue(String query) throws RemoteException {
		synchronized (queryQueue) {
			System.out.println("Adding to queue:" + query);
			queryQueue.add(query);
			queryQueue.notify();
		}
	}

	@Override
	public void addQueriesToQueue(List<String> queries) throws RemoteException {
		synchronized (queryQueue) {
			queryQueue.addAll(queries);
			queryQueue.notify();
		}
	}
	
}
