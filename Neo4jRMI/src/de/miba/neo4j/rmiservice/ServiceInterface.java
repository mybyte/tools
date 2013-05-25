package de.miba.neo4j.rmiservice;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface ServiceInterface extends Remote {

	public void addQueryToQueue(String query) throws RemoteException;
	public void addQueriesToQueue(List<String> queries) throws RemoteException;
	
}
