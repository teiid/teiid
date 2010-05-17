/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.xml.streaming;

import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.handler.MessageContext;

import org.jdom.Element;
import org.jdom.output.XMLOutputter;
import org.teiid.core.util.Assertion;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.Constants;
import org.teiid.translator.xml.CriteriaDesc;
import org.teiid.translator.xml.Document;
import org.teiid.translator.xml.DocumentBuilder;
import org.teiid.translator.xml.ExecutionInfo;
import org.teiid.translator.xml.OutputXPathDesc;
import org.teiid.translator.xml.ParameterDescriptor;
import org.teiid.translator.xml.QueryAnalyzer;
import org.teiid.translator.xml.RequestGenerator;
import org.teiid.translator.xml.XMLExecutionFactory;
import org.teiid.translator.xml.XMLPlugin;


public class BaseStreamingExecution implements ResultSetExecution {
    public static final String PARM_INPUT_XPATH_TABLE_PROPERTY_NAME = "XPathRootForInput"; //$NON-NLS-1$
    public static final String PARM_INPUT_NAMESPACE_TABLE_PROPERTY_NAME = "NamespaceForDocument"; //$NON-NLS-1$
	public final static String soapNSLabel = "SOAP-ENV"; //$NON-NLS-1$
	public static final String soapHeader= "Header"; //$NON-NLS-1$
	public static final String DUMMY_NS_PREFIX = "mm-dummy";  //$NON-NLS-1$	
	public static final String DUMMY_NS_NAME = "http://www.teiid.org/dummy";  //$NON-NLS-1$

	
	private List<Object[]> results = new ArrayList<Object[]>();
	private int resultIndex = 0;

	// injected state
	protected RuntimeMetadata metadata;
	protected ExecutionContext context;
	protected XMLExecutionFactory executionFactory;
	Dispatch<Source> dispatch;
	ExecutionInfo executionInfo;
	private Source soapPayload;
	Iterator<Document> resultsIterator;

	public BaseStreamingExecution(Select command, RuntimeMetadata rtMetadata, ExecutionContext context, XMLExecutionFactory executionFactory, Dispatch<Source> dispatch) throws TranslatorException {
		this.metadata = rtMetadata;
		this.context = context;
		this.executionFactory = executionFactory;
		this.dispatch = dispatch;
		
		QueryAnalyzer analyzer  = new QueryAnalyzer(command);
		this.executionInfo = analyzer.getExecutionInfo();
		List<CriteriaDesc[]> requestPerms = analyzer.getRequestPerms();

		for (CriteriaDesc[] criteria : requestPerms) {
			processOutputXPathDescs(this.executionInfo.getRequestedColumns(), Arrays.asList(criteria));
		}
		
        if (checkIfRequestIsNeeded(this.executionInfo)) {            
    		for (CriteriaDesc[] criteria : requestPerms) {
            	List<CriteriaDesc[]> parameters = RequestGenerator.getRequests(Arrays.asList(criteria));
            	
            	// Build a query String for http
            	String queryString = buildQueryString(parameters);
            	this.dispatch.getRequestContext().put(MessageContext.QUERY_STRING, queryString);
            	            	            	
            	String endpoint = buildAlternateEndpoint(this.executionInfo);
            	if (endpoint == null) {
            		this.dispatch.getRequestContext().put(Dispatch.ENDPOINT_ADDRESS_PROPERTY, endpoint);
            	}
            	else {
            		String pathInfo = buildPath(this.executionInfo);
            		if (pathInfo != null) {
            			this.dispatch.getRequestContext().put(MessageContext.PATH_INFO, pathInfo);
            		}
            	}
    		}        	
    		
    		String soapAction = this.executionInfo.getOtherProperties().get("SOAPAction"); //$NON-NLS-1$
    		if (soapAction != null) {
				dispatch.getRequestContext().put(Dispatch.SOAPACTION_USE_PROPERTY, Boolean.TRUE);
				dispatch.getRequestContext().put(Dispatch.SOAPACTION_URI_PROPERTY, soapAction);
    		}
    		
        
	    	try {
				// Build XML string for HTTP
				String xmlPayload = buildSimpleXML(requestPerms);
				if (xmlPayload != null) {
					Map<String, Object> map = (Map)this.dispatch.getRequestContext().get(MessageContext.INBOUND_MESSAGE_ATTACHMENTS);
					if (map == null) {
						map = new HashMap<String, Object>();
					}
					map.put("xml", xmlPayload); //$NON-NLS-1$
					this.dispatch.getRequestContext().put(MessageContext.INBOUND_MESSAGE_ATTACHMENTS, map);
			        if (executionFactory.isLogRequestResponseDocs()) {
						LogManager.logDetail(LogConstants.CTX_CONNECTOR, xmlPayload);
			        }
				}
			} catch (TranslatorException e) {
				LogManager.logDetail(LogConstants.CTX_CONNECTOR, XMLPlugin.Util.getString("failed_to_generate_xml_request")); //$NON-NLS-1$
			}

			try {
				// Build XML for SOAP
				String soapIn = buildSOAPInput(requestPerms);
				if(soapIn != null) {
					this.soapPayload = new StreamSource(new StringReader(soapIn));
			        if (executionFactory.isLogRequestResponseDocs()) {
						LogManager.logDetail(LogConstants.CTX_CONNECTOR, soapIn);
			        }									
				}
			} catch (TranslatorException e) {
				LogManager.logDetail(LogConstants.CTX_CONNECTOR, XMLPlugin.Util.getString("failed_to_generate_xml_request")); //$NON-NLS-1$
			}
        }        
	}
	
	protected void initialize() {
		
	}

	private String buildQueryString(List<CriteriaDesc[]> parameters) throws TranslatorException{
		StringBuilder sb  = new StringBuilder();
		
        for (CriteriaDesc[] query: parameters) {
        	for(CriteriaDesc cd: query) {
                String name = (cd.getInputXpath() == null || cd.getInputXpath().length() == 0) ? cd.getColumnName() : cd.getInputXpath();
                sb.append(name).append("=").append(cd.getCurrentIndexValue()).append("&"); //$NON-NLS-1$ //$NON-NLS-2$
        	}
        }
        return sb.toString();
	}
	
	
	public void cancel() throws TranslatorException {
		// nothing to do
	}
	
	private List<String> getXPaths() {
        XPathSplitter splitter = new XPathSplitter();
        try {
			return splitter.split(this.executionInfo.getTableXPath());
		} catch (InvalidPathException e) {
			e.printStackTrace();
		}		
		return null;
	}
	/**
	 * Earlier implementations retrieved the XML in the execute method.  Because this can be any
	 * number of documents of any size, this caused memory problems because the xml was 
	 * completely realized in memory.  In this impl the setup work is done in execute and
	 * the xml is streamed in the next function.
	 */
	public List next() throws TranslatorException, DataNotAvailableException {
		this.context.keepExecutionAlive(true);
		
		List result = null;
		if (this.resultIndex == 0) {
			fillInResults();
		}
		
		if(resultIndex < results.size()) {
			result = Arrays.asList(results.get(resultIndex));
			++resultIndex;
		}
		return result;
	}

	private void fillInResults() throws TranslatorException {
		List<Object[]> rows;
		StreamingResultsProducer streamProducer = new StreamingResultsProducer(this.executionInfo, this.executionFactory.getSaxFilterProvider());
		Iterator<Document> streamIter = this.resultsIterator;
		while (streamIter.hasNext()) {
			Document xml = streamIter.next();
			// TODO: add stream filter class. --rareddy
			rows = streamProducer.getResult(xml, getXPaths());
			if (rows.isEmpty()) {
				continue;
			}
			results.addAll(rows);
		}
	}
	
    protected SQLXML convertToXMLType(Source value) {
    	return (SQLXML)this.executionFactory.getTypeFacility().convertToRuntimeType(value);
    }	
    
    @Override
    public void close() throws TranslatorException {
    	this.executionFactory.removeResponse(this.context.getExecutionCountIdentifier());
    }

	@Override
	public void execute() throws TranslatorException {
		ArrayList<Document> result = new ArrayList<Document>();
		result.add(getDocumentStream(this.executionInfo));
		this.resultsIterator = result.iterator();
	}
    
	/**
	 * Examines the Query to determine if a request to a source is needed.  If any of the 
	 * request parameters is a ResponseIn, then we don't need to make a request because it 
	 * has already been made by another call to Execution.execute()
	 */ 
    static boolean checkIfRequestIsNeeded(ExecutionInfo info) throws TranslatorException {
    	List cols = info.getRequestedColumns();
    	boolean retVal = true;
    	Iterator paramIter = cols.iterator();
    	while(paramIter.hasNext()) {
    		ParameterDescriptor desc = (ParameterDescriptor) paramIter.next();
    		if(desc.getRole().equalsIgnoreCase(ParameterDescriptor.ROLE_COLUMN_PROPERTY_NAME_RESPONSE_IN)) {
    			retVal = false;
    			break;
    		}
    	}
    	return retVal;
    }
    
    protected String buildAlternateEndpoint(ExecutionInfo executionInfo) {
	    String location = executionInfo.getLocation();
	    if (location != null) {
	        // If the location is a URL, it replaces the full URL (first part
	        // set in the
	        // connector binding and second part set in the model).
	        try {
	            new URL(location);
	            return location;
	        } catch (MalformedURLException e) {
	        }
	    }
	    return null;
    }
    
	protected String buildPath(ExecutionInfo executionInfo) {
	    String location = executionInfo.getLocation();
	    if (location == null) {
	        final String tableServletCallPathProp = "ServletCallPathforURL"; //$NON-NLS-1$
	        location = executionInfo.getOtherProperties().get(tableServletCallPathProp);
	    }
	    return location;
	}    
	
	/**
	 * Because of the structure of relational databases it is a simple and common practice
	 * to return the vaule of a critera in a result set.  For instance, 
	 * SELECT name, ssn from people where ssn='xxx-xx-xxxx'
	 * In a Request/Response XML scenario, there is no guarantee that ssn is in the response.  
	 * In most cases it will not be.  In order to meet the relational users expectation that
	 * the value for a select critera can be returned we stash the value from the parameter 
	 * in the output value and then fetch it when gathering results if possible. In some cases
	 * this is not possible, and in those cases we throw a TranslatorException. Implementations
	 * of this class can override cannotProjectParameter(CriteriaDesc parmCriteria) to make the 
	 * determination.
	 */
	private void processOutputXPathDescs(final List<OutputXPathDesc> columns, final List<CriteriaDesc> parameters) throws TranslatorException {
	    for (OutputXPathDesc column:columns) {
	        if (column.isParameter() && column.getXPath() == null) {
	        	int colNum = column.getColumnNumber();
	            for(CriteriaDesc cd:parameters) {
	    	        if (cd.getColumnNumber() == colNum) {
	    	            if (cannotProjectParameter(cd)) {
	    	                throw new TranslatorException(XMLPlugin.getString("HTTPExecutor.cannot.project.repeating.values")); //$NON-NLS-1$
	    	            } 
	    	            column.setCurrentValue(cd.getCurrentIndexValue());
	                    break;
	    	        }	            	
	            }
	        }
	    }
	}
	
    private boolean cannotProjectParameter(CriteriaDesc parmCriteria) {
        return parmCriteria.getNumberOfValues() > 1
                && parmCriteria.isUnlimited();
        		// this info is only available in the connection side
                //&& !((getState()).getParameterMethod() == HTTPConnectorState.PARAMETER_NAME_VALUE);
    }
	

	private Document getDocumentStream(ExecutionInfo executionInfo) {
		Document document;

        // Is this a request part joining across a document
        CriteriaDesc criterion = executionInfo.getResponseIDCriterion();
        if (criterion != null) {
            String responseid = (String) (criterion.getValues().get(0));
            SQLXML xml = this.executionFactory.getResponse(responseid);
            Assertion.isNotNull(xml);
        	document = new DocumentImpl(xml, responseid);
        } else {
        	// Not a join, but might still be cached.
        	// Not cached, so make the request
        	Source result = this.dispatch.invoke(this.soapPayload);
            SQLXML responseBody = this.executionFactory.convertToXMLType(result);
            if (executionFactory.isLogRequestResponseDocs()) {
            	try {
					LogManager.logDetail(LogConstants.CTX_CONNECTOR, responseBody.getString());
				} catch (SQLException e) {
				}
            }
    		this.executionFactory.setResponse(this.context.getExecutionCountIdentifier(), responseBody);
            document = new DocumentImpl(responseBody, this.context.getExecutionCountIdentifier());
        }
		return document;
	}    	
	
	
	private String buildSOAPInput(List<CriteriaDesc[]> params) throws TranslatorException {
		List<CriteriaDesc[]> requestPerms = RequestGenerator.getRequests(Arrays.asList(params.get(0)));
		CriteriaDesc[] parameters = requestPerms.get(0);
		
		List<CriteriaDesc> queryList = new ArrayList<CriteriaDesc>(Arrays.asList(parameters));
		List<CriteriaDesc> headerParams = new ArrayList<CriteriaDesc>();
		List<CriteriaDesc> bodyParams = new ArrayList<CriteriaDesc>();

		for (CriteriaDesc desc : queryList) {
			if (desc.getInputXpath().startsWith(soapNSLabel+ ":" + soapHeader)) { //$NON-NLS-1$
				headerParams.add(desc);
			} else {
				bodyParams.add(desc);
			}
		}		
		
		String namespacePrefixes = this.executionInfo.getOtherProperties().get(Constants.NAMESPACE_PREFIX_PROPERTY_NAME);
		String inputParmsXPath = this.executionInfo.getOtherProperties().get(DocumentBuilder.PARM_INPUT_XPATH_TABLE_PROPERTY_NAME); 

		DocumentBuilder builder = new DocumentBuilder();
		//TODO: always set to encoded false - rareddy
		builder.setUseTypeAttributes(false);
		final String slash = "/"; //$NON-NLS-1$
		final String dotSlash = "./"; //$NON-NLS-1$
		boolean hasDummy = false;
		if (inputParmsXPath.equals(dotSlash) || inputParmsXPath.equals(slash) || inputParmsXPath.equals("")) { //$NON-NLS-1$
			inputParmsXPath = DUMMY_NS_PREFIX + ":dummy"; //$NON-NLS-1$
			namespacePrefixes = namespacePrefixes + " xmlns:" + DUMMY_NS_PREFIX + "=\"" + DUMMY_NS_NAME + "\""; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			hasDummy = true;
		}
		org.jdom.Document doc = builder.buildDocument(Arrays.asList(parameters), inputParmsXPath, namespacePrefixes);
		if (hasDummy) {
			// Since there is no real root - these should all be elements
			Element element = (Element) doc.getRootElement().getChildren().get(0);
			element.detach();
			doc = new org.jdom.Document(element);
		}
		return new XMLOutputter().outputString(doc);
	}
	
	private String buildSimpleXML(List<CriteriaDesc[]> params) throws TranslatorException{
		List<CriteriaDesc[]> requestPerms = RequestGenerator.getRequests(Arrays.asList(params.get(0)));
		CriteriaDesc[] parameters = requestPerms.get(0);
		
		DocumentBuilder builder = new DocumentBuilder();
    	Map<String, String> props = executionInfo.getOtherProperties();
        String inputParmsXPath = props.get(DocumentBuilder.PARM_INPUT_XPATH_TABLE_PROPERTY_NAME);
        String namespacePrefixes = props.get(Constants.NAMESPACE_PREFIX_PROPERTY_NAME);
		
        org.jdom.Document inputXMLDoc = builder.buildDocument(Arrays.asList(parameters), inputParmsXPath,namespacePrefixes);
        XMLOutputter out = new XMLOutputter();
        return out.outputString(inputXMLDoc).trim();
	}		
	
}