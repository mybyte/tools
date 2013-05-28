package de.miba.neo4j.loader.turtle;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

public class Neo4jHandler implements RDFHandler {

	private int totalNodes = 0;
	private int sinceLastCommit = 0;

	private long tick = System.currentTimeMillis();
	private GraphDatabaseService db;
	private Index<Node> index;

	private Transaction tx;

	public Neo4jHandler(GraphDatabaseService db) {
		this.db = db;
		index = db.index().forNodes("ttlIndex");
		tx = db.beginTx();
	}

	@Override
	public void handleStatement(Statement st) {
		try {
			Resource subject = st.getSubject();
			URI predicate = st.getPredicate();
			String predicateName = predicate.getLocalName();

			Value object = st.getObject();

			// Check index for subject
			Node subjectNode;
			IndexHits<Node> hits = index.get("resource", subject.stringValue());
			if (hits.hasNext()) { // node exists
				subjectNode = hits.next();
			} else {
				subjectNode = db.createNode();
				subjectNode.setProperty("__URI__", subject.stringValue());
				index.add(subjectNode, "resource", subject.stringValue());
			}

			// add Label if this is a type entry
			if (predicate.stringValue().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
				String label = ((URI) object).getLocalName();
				boolean hit = false;
				for (Label lbl : subjectNode.getLabels())
					if (label.equals(lbl)) {
						hit = true;
						break;
					}
				if (!hit)
					subjectNode.addLabel(DynamicLabel.label(label));

			} else if (object instanceof Literal) {
				URI type = ((Literal) object).getDatatype();
				Object value;
				if (type == null) // treat as String
					value = object.stringValue();
				else {
					String localName = type.getLocalName();

					if (localName.toLowerCase().contains("integer") || localName.equals("long")) {
						value = ((Literal) object).longValue();
					} else if (localName.toLowerCase().contains("short")) {
						value = ((Literal) object).shortValue();
					} else if (localName.equals("byte")) {
						value = ((Literal) object).byteValue();
					} else if (localName.equals("char")) {
						value = ((Literal) object).byteValue();
					} else if (localName.equals("float")) {
						value = ((Literal) object).floatValue();
					} else if (localName.equals("double")) {
						value = ((Literal) object).doubleValue();
					} else if (localName.equals("boolean")) {
						value = ((Literal) object).booleanValue();
					} else {
						value = ((Literal) object).stringValue();
					}
				}

				subjectNode.setProperty(predicateName, value);

			} else { // must be Resource
				// Make sure object exists
				Node objectNode;

				hits = index.get("resource", object.stringValue());
				if (hits.hasNext()) { // node exists
					objectNode = hits.next();
				} else {
					objectNode = db.createNode();
					objectNode.setProperty("__URI__", object.stringValue());
					index.add(objectNode, "resource", object.stringValue());
				}

				// Make sure this relationship is unique
				RelationshipType relType = DynamicRelationshipType.withName(predicateName);
				boolean hit = false;
				for (Relationship rel : subjectNode.getRelationships(Direction.OUTGOING, relType)) {
					if (rel.getEndNode().equals(objectNode)) {
						hit = true;
					}
				}

				if (!hit) { // Only create relationship, if it didn't exist
					subjectNode.createRelationshipTo(objectNode, DynamicRelationshipType.withName(predicateName)).setProperty("__URI__",
							predicate.stringValue());
				}
			}

			totalNodes++;

			long nodeDelta = totalNodes - sinceLastCommit;
			long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			if (nodeDelta >= 150000 || timeDelta >= 30) { // Commit every 150k operations or every 30 seconds
				tx.success();
				tx.finish();
				tx = db.beginTx();

				sinceLastCommit = totalNodes;
				System.out.println(totalNodes + " triples @ ~" + (double) nodeDelta / timeDelta + " triples/second.");
				tick = System.currentTimeMillis();
			}
		} catch (Exception e) {
			e.printStackTrace();
			tx.finish();
			tx = db.beginTx();
		}
	}

	public int getCountedStatements() {
		return totalNodes;
	}

	@Override
	public void endRDF() throws RDFHandlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleComment(String arg0) throws RDFHandlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
		// TODO Auto-generated method stub

	}

	@Override
	public void startRDF() throws RDFHandlerException {
		// TODO Auto-generated method stub

	}

}
