package de.miba.neo4j.loader.turtle;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchRelationship;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

public class Neo4jBatchHandler implements RDFHandler {

	private int totalTriples = 0;
	private int triplesLastCommit = 0;
	private int addedNodes = 0;
	private int addedLabels = 0;
	private int addedRelationships = 0;

	private int indexCache;
	private int timeout;

	private long tick = System.currentTimeMillis();
	private BatchInserter db;
	private BatchInserterIndex index;
	
	private static final String URI_PROPERTY="__URI__";

	private Map<String, Long> tmpIndex = new HashMap<String, Long>();

	public Neo4jBatchHandler(BatchInserter db2, int indexCache, int timeout) {
		this.db = db2;
		this.indexCache = indexCache;
		this.timeout = timeout;

		BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db);
		index = indexProvider.nodeIndex("ttlIndex", MapUtil.stringMap("type", "exact"));
		index.setCacheCapacity(URI_PROPERTY, indexCache + 1);

	}

	public void handleStatement(Statement st) {
		try {
			Resource subject = st.getSubject();
			URI predicate = st.getPredicate();
			String predicateName = predicate.getLocalName();

			Value object = st.getObject();

			// Check index for subject
			Long subjectNode = tmpIndex.get(subject.stringValue());

			if (subjectNode == null) {
				IndexHits<Long> hits = index.get(URI_PROPERTY, subject.stringValue());
				if (hits.hasNext()) { // node exists
					subjectNode = hits.next();
				} else {
					Map<String, Object> props = new HashMap<String, Object>();
					props.put(URI_PROPERTY, subject.stringValue());
					subjectNode = db.createNode(props);

					tmpIndex.put(subject.stringValue(), subjectNode);

					index.add(subjectNode, props);
					addedNodes++;
				}
			}
			// add Label if this is a type entry
			if (predicate.stringValue().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
				Label label = DynamicLabel.label(((URI) object).getLocalName());
				boolean hit = false;
				Iterable<Label> labels = db.getNodeLabels(subjectNode);
				List<Label> labelList = new LinkedList<Label>();
				for (Label lbl : labels) {
					if (label.equals(lbl)) {
						hit = true;
						break;
					}
					labelList.add(lbl);
				}
				if (!hit) {
					labelList.add(label);
					db.setNodeLabels(subjectNode, labelList.toArray(new Label[labelList.size()]));
					addedLabels++;
				}

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
				
				Map<String, Object> nodeProps = db.getNodeProperties(subjectNode);
				nodeProps.put(predicateName, value);
				db.setNodeProperties(subjectNode, nodeProps);

			} else { // must be Resource
				// Make sure object exists
				Long objectNode = tmpIndex.get(object.stringValue());

				if (objectNode == null) {
					IndexHits<Long> hits = index.get(URI_PROPERTY, object.stringValue());
					if (hits.hasNext()) { // node exists
						objectNode = hits.next();
					} else {
						Map<String, Object> props = new HashMap<String, Object>();
						props.put(URI_PROPERTY, object.stringValue());
						objectNode = db.createNode(props);

						tmpIndex.put(object.stringValue(), objectNode);

						index.add(objectNode, props);
						addedNodes++;
					}
				}

				// Make sure this relationship is unique
				RelationshipType relType = DynamicRelationshipType.withName(predicateName);
				boolean hit = false;
				for (BatchRelationship rel : db.getRelationships(subjectNode)) {
					if (rel.getEndNode() == (objectNode) && rel.getType().equals(relType)) {
						hit = true;
						break;
					}
				}

				if (!hit) { // Only create relationship, if it didn't exist
					Map<String, Object> props = new HashMap<String, Object>();
					props.put(URI_PROPERTY, predicate.stringValue());
					db.createRelationship(subjectNode, objectNode, relType, props);
					addedRelationships++;
				}
			}

			totalTriples++;

			long nodeDelta = totalTriples - triplesLastCommit;
			long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			// periodical flush
			if (nodeDelta >= indexCache || timeDelta >= timeout) {
				index.flush();
				// clear HashMap after flushing
				tmpIndex.clear();
				
				triplesLastCommit = totalTriples;
				System.out.println(totalTriples + " triples, "+addedNodes+" nodes @ ~" + nodeDelta / timeDelta + " triples/second.");
				tick = System.currentTimeMillis();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int getCountedStatements() {
		return totalTriples;
	}
	
	public int getNodesAdded(){
		return addedNodes;
	}

	public void startRDF() throws RDFHandlerException {
		// TODO Auto-generated method stub
		
	}

	public void endRDF() throws RDFHandlerException {
		// TODO Auto-generated method stub
		
	}

	public void handleNamespace(String paramString1, String paramString2) throws RDFHandlerException {
		// TODO Auto-generated method stub
		
	}

	public void handleComment(String paramString) throws RDFHandlerException {
		// TODO Auto-generated method stub
		
	}
}
