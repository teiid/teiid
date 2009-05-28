package com.metamatrix.connector.xml.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xml.Document;
import com.metamatrix.connector.xml.ResultProducer;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.OutputXPathDesc;
import com.metamatrix.connector.xml.base.QueryAnalyzer;
import com.metamatrix.connector.xml.base.XMLConnectionImpl;
import com.metamatrix.connector.xml.cache.CachedXMLStream;
import com.metamatrix.connector.xml.streaming.BaseStreamingExecution;
import com.metamatrix.connector.xml.streaming.DocumentImpl;
import com.metamatrix.connector.xml.streaming.InvalidPathException;
import com.metamatrix.connector.xml.streaming.StreamingResultsProducer;
import com.metamatrix.connector.xml.streaming.XPathSplitter;

public class FileExecution extends BaseStreamingExecution implements ResultProducer {

	private String[] docs;
	private String directory;
	private FileConnectorState state;
	public static final String PARM_FILE_NAME_TABLE_PROPERTY_NAME = "FileName"; //$NON-NLS-1$
	
	public FileExecution(IQuery command, XMLConnectionImpl conn, RuntimeMetadata metadata,
			ExecutionContext context, ConnectorEnvironment env) throws ConnectorException {
		super(command, conn, metadata, context, env);
		state = (FileConnectorState) conn.getState();
		logger = getConnection().getState().getLogger();
	}

	@Override
	public void execute() throws ConnectorException {
			XMLConnectorState state = getConnection().getState();

			QueryAnalyzer analyzer = new QueryAnalyzer(query, metadata, state
					.getPreprocessor(), logger, getExeContext(), connEnv);
			exeInfo = analyzer.getExecutionInfo();
			init(); // depends upon the creation of m_info
			validateParams();
			List requestPerms = analyzer.getRequestPerms();

			if (requestPerms.size() > 1) {
				throw new AssertionError(
						"The QueryAnalyzer produced > 1 request permutation");
			}

			List<CriteriaDesc> criteriaList = Arrays.asList((CriteriaDesc[]) requestPerms.get(0));
			getInfo().setParameters(criteriaList);
            
            XPathSplitter splitter = new XPathSplitter();
            try {
				xpaths = splitter.split(getInfo().getTableXPath());
			} catch (InvalidPathException e) {
				e.printStackTrace();
			}
			
			rowProducer = new StreamingResultsProducer(getInfo(), state);
			resultProducers.add(getStreamProducer());
	}
	
	/**
	 * Validates that the query can be supported.  Probably better suited to a call out from QueryAnalyzer.
	 * @throws ConnectorException
	 */
	private void validateParams() throws ConnectorException {
        for (int i = 0; i < getInfo().getRequestedColumns().size(); i++) {
            OutputXPathDesc xPath = (OutputXPathDesc) getInfo().getRequestedColumns().get(i);
            if (xPath.isParameter()) {
                throw new ConnectorException(
                		com.metamatrix.connector.xml.file.Messages.getString("FileExecutor.input.not.supported.on.files")); //$NON-NLS-1$
            }
        }
    }

	
	/////////////////////////
	// Begin Initialization
	/////////////////////////
	
	private void init() throws ConnectorException {
		String tableFileName = getTableFileName();
        String xmlFileName = state.getFileName();
        if (tableFileName != null && tableFileName.trim().length() == 0) {
        	tableFileName = null;
        }        
        if (xmlFileName.trim().length() == 0) {
        	xmlFileName = null;
        }
        String xmlFileDir = state.getDirectoryPath();

        directory = normalizePath(xmlFileDir);

        if (tableFileName == null && xmlFileName == null) {
            validateDirectory();
        } else {
            validateFile(tableFileName, xmlFileName, xmlFileDir);
        }
	}
	
	private String getTableFileName() {
		String retval = getInfo().getLocation();
        if (retval == null) {
            retval = getInfo().getOtherProperties().getProperty(PARM_FILE_NAME_TABLE_PROPERTY_NAME);
        }
		return retval;
	}
	
    /**
     * Initializes the internal list of XML Documents.  Throws a ConnectorException in the 
     * event that the directory contains no .xml files.
     * @throws ConnectorException
     */
    private void validateDirectory() throws ConnectorException {
        File dirFile = new File(directory);
        String[] files = dirFile.list();
        ArrayList<String> xmlFiles = new ArrayList<String>();
        for( int i = 0; i < files.length; i++) {
        	boolean valid = validateFile(files[i], directory, true);
        	if(valid) {
        		xmlFiles.add(files[i]);
        	}
        }
        files = new String[xmlFiles.size()];
        xmlFiles.toArray(files);
        docs = files;
        if (docs.length <= 0) {
            throw new ConnectorException(
            		com.metamatrix.connector.xml.file.Messages.getString("FileExecutor.empty.directory")); //$NON-NLS-1$
            }
    }

    private boolean validateFile(String fileName, String directory, boolean validateExtension) {
        boolean result = false;
    	String myXmlFileName = normalizePath(directory) + fileName;
        File xmlFile = new File(myXmlFileName);
        if (xmlFile.isFile()) {
            if (validateExtension) {
                if (myXmlFileName.toLowerCase().endsWith(".xml")) { //$NON-NLS-1$
                    result = true;
                }
            } else {
                result = true;
            }
        }
        return result;
	}

	private void validateFile(String tableFileName, String xmlFileName,
            String xmlFileDir) throws ConnectorException {
        docs = new String[1];
        docs[0] = (tableFileName == null) ? xmlFileName : tableFileName;
        boolean valid = validateFile(docs[0], xmlFileDir, false);
        if (!valid) {
            throw new ConnectorException(
            		com.metamatrix.connector.xml.file.Messages.getString("FileExecutor.not.file")); //$NON-NLS-1$
        }
    }
	
	private String normalizePath(String path) {
        if (path.endsWith(File.separator)) {
            return path;            
        } else {
            return new String(path + File.separator);
        }
    }
	/////////////////////////
	// End Initialization
	/////////////////////////
	
	private class XMLFileIterator implements Iterator<Document> {

		private String queryID;
		private int docNumber;

		public XMLFileIterator(String id) throws ConnectorException {
			if(null == id) {
				throw new ConnectorException(
						Messages.getString("FileExecution.null.query.id"));
			}
			queryID = id;
		}
		
		@Override
		public boolean hasNext() {
			return docNumber < docs.length;
		}

		@Override
		public Document next() {
			if(!hasNext()) {
				throw new NoSuchElementException();
			}
			Document document;
			try {
				document = getDocument();
			} catch (ConnectorException e) {
				throw new NoSuchElementException(e.getMessage());
			}
			++docNumber;
			return document;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		private Document getDocument() throws ConnectorException {
			Document doc;
			String cacheKey = queryID + new Integer(docNumber).toString();
			if(state.isCaching()) {
				if(null != exeContext.get(queryID)) {
					InputStream stream = new CachedXMLStream(exeContext, queryID);
					doc = new DocumentImpl(stream, cacheKey);
					logger.logTrace("Got " + queryID + " from the cache");
				} else {
					InputStream stream = getDocumentStream(docNumber);
					stream = state.addCachingStreamFilters(stream, exeContext, queryID);
					doc = new DocumentImpl(stream, cacheKey); 
				}
			} else {
				InputStream stream = getDocumentStream(docNumber);
				stream = state.addStreamFilters(stream);
				doc = new DocumentImpl(stream, cacheKey);
 			}
			return doc;
		}
		
		private InputStream getDocumentStream(int i) throws ConnectorException {
			try {
				String xmlFileName = directory + docs[i];
				File xmlFile = new File(xmlFileName);
				logger.logTrace(
								"XML Connector Framework: retrieving document from " + xmlFileName); //$NON-NLS-1$
				InputStream retval = new FileInputStream(xmlFile);
				logger.logTrace(
						"XML Connector Framework: retrieved file " + xmlFileName); //$NON-NLS-1$
				return retval;
			} catch (IOException ioe) {
				throw new ConnectorException(ioe);
			}
		}

	}

	public ExecutionInfo getInfo() {
		return exeInfo;
	}

	@Override
	public ResultProducer getStreamProducer() throws ConnectorException {
		return this;
	}

	public Iterator<Document> getXMLDocuments() throws ConnectorException {
		return new XMLFileIterator(state.getDirectoryPath());
	}

	@Override
	public void closeStreams() {
		// Nothing to do
	}
}
