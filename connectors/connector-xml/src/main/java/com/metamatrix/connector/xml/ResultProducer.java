package com.metamatrix.connector.xml;

import java.util.Iterator;

import org.teiid.connector.api.ConnectorException;

public interface ResultProducer {

	/**
	 * Gets all the InputStreams for a single ExecutionInfo instance.
	 * This could be any number or streams and is implementation dependent.
	 * @return
	 * @throws ConnectorException 
	 */
	public Iterator<Document>  getXMLDocuments() throws ConnectorException;
	
	public void closeStreams();
}
