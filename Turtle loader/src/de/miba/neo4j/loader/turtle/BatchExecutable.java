package de.miba.neo4j.loader.turtle;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.turtle.TurtleParser;

public class BatchExecutable {

	/**
	 * @param launchParams
	 * @throws Exception
	 */
	public static void main(String[] launchParams) throws Exception {
		if (launchParams.length < 1) {
			throw new Exception("Provide a file path as a parameter!");
		}

		BatchInserter db = null;
		try {
			Map<String, String> config = new HashMap<String, String>();
			config.put("neostore.nodestore.db.mapped_memory", "500M");
			db = BatchInserters.inserter("ttl.db", config);

			RDFParser rdfParser = new TurtleParser();

			Neo4jBatchHandler handler = new Neo4jBatchHandler(db, 500000, 60);
			rdfParser.setRDFHandler(handler);

			for (String path : launchParams) {
				File file = new File(path);
				BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
				rdfParser.parse(bis, file.toURI().toString());
			}

			System.out.println(handler.getCountedStatements());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			db.shutdown();
		}
	}

}
