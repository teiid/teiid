package com.metamatrix.connector.xml.streaming;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xml.Document;
import com.metamatrix.connector.xml.ResultProducer;
import com.metamatrix.connector.xml.XMLConnection;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.XMLConnectionImpl;

public abstract class BaseStreamingExecution implements XMLExecution {

	protected List<String> xpaths;
	protected StreamingResultsProducer rowProducer;
	protected List<Object[]> results;
	private int resultIndex;
	private Iterator<Document> streamIter;
	protected List<ResultProducer> resultProducers;
	private Iterator<ResultProducer> producerIter;
	protected ExecutionInfo exeInfo;
	protected IQuery query;
	protected RuntimeMetadata metadata;
	protected ExecutionContext exeContext;
	protected ConnectorEnvironment connEnv;
	protected XMLConnectionImpl connection;
	protected ConnectorLogger logger;

	public BaseStreamingExecution(IQuery command, XMLConnectionImpl conn,
			RuntimeMetadata rtMetadata, ExecutionContext context,
			ConnectorEnvironment env) {
		query = command;
		connection = conn;
		metadata = rtMetadata;
		exeContext = context;
		connEnv = env;
		logger = connection.getConnectorEnv().getLogger();
		resultProducers = new ArrayList<ResultProducer>();
		results = new ArrayList<Object[]>();
	}

	public void cancel() throws ConnectorException {
		// nothing to do
	}

	public void close() throws ConnectorException {
		// nothing to do
	}

	/**
	 * Earlier implementations retrieved the XML in the execute method.  Because this can be any
	 * number of documents of any size, this caused memory problems because the xml was 
	 * completely realized in memory.  In this impl the setup work is done in execute and
	 * the xml is streamed in the next function.
	 */
	public List next() throws ConnectorException, DataNotAvailableException {
		getExeContext().keepExecutionAlive(true);
		List result = null;
		if(resultIndex < results.size()) {
			result = Arrays.asList(results.get(resultIndex));
			++resultIndex;
		} else  {
			List rows;
			File xmlFile;
			Document xml;
			if(null == streamIter) {
				if(null == producerIter) {
					producerIter = resultProducers.iterator();
				}
				if(producerIter.hasNext()) {
					ResultProducer resultProducer = producerIter.next();
					streamIter = resultProducer.getXMLDocuments();
				}
			}
			while (streamIter.hasNext()) {
				xml = streamIter.next();
				rows = rowProducer.getResult(xml, xpaths);
				if (rows.isEmpty()) {
					continue;
				}
				results = rows;
				resultIndex = 0;
			}
			if(resultIndex < results.size()) {
				result = Arrays.asList(results.get(resultIndex));
				++resultIndex;
			}
		}
		return result;
	}

	public ExecutionContext getExeContext() {
		return exeContext;
	}

	public XMLConnection getConnection() {
		return connection;
	}

}