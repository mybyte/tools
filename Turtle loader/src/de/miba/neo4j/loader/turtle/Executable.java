package de.miba.neo4j.loader.turtle;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.turtle.TurtleParser;

public class Executable {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(args.length!=1){
			throw new Exception("Provide a file path as a parameter!");
		}
		
		File file = new File(args[0]);
		
		if(!file.canRead())
			throw new Exception("Can't read the file.");
				
		HashMap<String, String> settings = new HashMap<>();
		settings.put("org.neo4j.server.database.mode", "HA");

		GraphDatabaseService db = new HighlyAvailableGraphDatabaseFactory()
				.newHighlyAvailableDatabaseBuilder("db.local")
				.loadPropertiesFromFile("neo4j.properties").setConfig(settings)
				.newGraphDatabase();
		
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
		
		RDFParser rdfParser = new TurtleParser();
		
		Neo4jHandler handler = new Neo4jHandler(db);
		rdfParser.setRDFHandler(handler);
		
		rdfParser.parse(bis, file.toURI().toString());
		
		System.out.println(handler.getCountedStatements());
	}

}
