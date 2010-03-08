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
package org.teiid.connector.xmlsource.soap;

import java.io.StringReader;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.XMLOutputter;
import org.teiid.connector.api.CacheScope;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.xmlsource.XMLSourcePlugin;

import com.metamatrix.connector.xml.Constants;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.DocumentBuilder;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.OutputXPathDesc;
import com.metamatrix.connector.xml.base.ParameterDescriptor;
import com.metamatrix.connector.xml.base.RequestGenerator;
import com.metamatrix.connector.xml.streaming.DocumentImpl;
import com.metamatrix.core.util.Assertion;

public class SOAPRequest extends BaseRequest {

	public static final String PARAMETER_NAME_VALUE = "Name/Value"; //$NON-NLS-1$
	
	/*SecurityToken secToken;*/
	XMLOutputter xmlOutputter = new XMLOutputter();
	Document requestDoc;
	private int requestNumber;
	private ExecutionInfo executionInfo;
	List<CriteriaDesc> parameters = null;
	ExecutionContext context;
	SoapManagedConnectionFactory config;

	public SOAPRequest(SoapManagedConnectionFactory config, ExecutionContext context, ExecutionInfo executionInfo, List<CriteriaDesc> parameters, int requestNumber) throws ConnectorException {
		super(config.getUri(), executionInfo);
		this.config = config;
		this.requestNumber = requestNumber;
		this.executionInfo = executionInfo;
		this.parameters = parameters;
		this.context = context;
		
		processOutputXPathDescs(executionInfo.getRequestedColumns(), parameters);
		this.requestDoc = createRequests();
	}
	
	/**
	 * Because of the structure of relational databases it is a simple and common practice
	 * to return the vaule of a critera in a result set.  For instance, 
	 * SELECT name, ssn from people where ssn='xxx-xx-xxxx'
	 * In a Request/Response XML scenario, there is no guarantee that ssn is in the response.  
	 * In most cases it will not be.  In order to meet the relational users expectation that
	 * the value for a select critera can be returned we stash the value from the parameter 
	 * in the output value and then fetch it when gathering results if possible. In some cases
	 * this is not possible, and in those cases we throw a ConnectorException. Implementations
	 * of this class can override cannotProjectParameter(CriteriaDesc parmCriteria) to make the 
	 * determination.
	 */
	private void processOutputXPathDescs(final List requestedColumns, final List parameterPairs) throws ConnectorException {
	    for (int i = 0; i < requestedColumns.size(); i++) {
	        OutputXPathDesc xPath = (com.metamatrix.connector.xml.base.OutputXPathDesc) requestedColumns.get(i);
	        if (xPath.isParameter() && xPath.getXPath() == null) {
	    	    int colNum = xPath.getColumnNumber();
	    	    // Put the input parameter value in the result column value if possible.
	    	    for (int x = 0; x < parameterPairs.size(); x++) {
	    	        CriteriaDesc parmCriteria = (CriteriaDesc) parameterPairs.get(x);
	    	        if (parmCriteria.getColumnNumber() == colNum) {
	    	            if (cannotProjectParameter(parmCriteria)) {
	    	                throw new ConnectorException(XMLSourcePlugin.Util.getString("HTTPExecutor.cannot.project.repeating.values")); //$NON-NLS-1$
	    	            } 
	                    xPath.setCurrentValue(parmCriteria.getCurrentIndexValue());
	                    break;
	    	        }
	    	    }  
	        }
	    }
	}	
	
    private boolean cannotProjectParameter(CriteriaDesc parmCriteria) {
        return parmCriteria.getNumberOfValues() > 1 && parmCriteria.isUnlimited() && !(this.config.getParameterMethod() == PARAMETER_NAME_VALUE);
    }	
    
	public com.metamatrix.connector.xml.Document getDocumentStream() throws ConnectorException {
		com.metamatrix.connector.xml.Document document;
		//ExecutionContext exeContext = execution.getExeContext();

        // Is this a request part joining across a document
        CriteriaDesc criterion = this.executionInfo.getResponseIDCriterion();
        if (criterion != null) {
            String responseid = (String) (criterion.getValues().get(0));
            SQLXML xml = (SQLXML)this.context.getFromCache(CacheScope.REQUEST, responseid);
            Assertion.isNotNull(xml);
        	document = new DocumentImpl(xml, responseid);
        } else {
        	// Not a join, but might still be cached.
        	// Not cached, so make the request
            SQLXML responseBody = executeRequest();
            this.context.storeInCache(CacheScope.REQUEST,this.context.getExecutionCountIdentifier(), responseBody);
            //InputStream filteredStream = getState().addStreamFilters(responseBody);
            document = new DocumentImpl(responseBody, this.context.getExecutionCountIdentifier());
        }
		return document;
	}
	
    private Document createRequests() throws ConnectorException {
        if (checkIfRequestIsNeeded(this.executionInfo)) {
            return setRequests(this.parameters);
        }
        return null;
    }	
    
	/**
	 * Examines the Query to determine if a request to a source is needed.  If any of the 
	 * request parameters is a ResponseIn, then we don't need to make a request because it 
	 * has already been made by another call to Execution.execute()
	 */ 
    static boolean checkIfRequestIsNeeded(ExecutionInfo info) throws ConnectorException {
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

	private SQLXML executeRequest() throws ConnectorException {
		try {
			/*
			 * TrustedPayloadHandler handler = execution.getConnection().getTrustedPayloadHandler();
			SecurityManagedConnectionFactory env = (SecurityManagedConnectionFactory)execution.getConnection().getConnectorEnv();
			secToken = SecurityToken.getSecurityToken(env, handler);
			*/
			
            QName svcQname = new QName("http://org.apache.cxf", "foo");
            QName portQName = new QName("http://org.apache.cxf", "bar");
            Service svc = Service.create(svcQname);
            svc.addPort(portQName, SOAPBinding.SOAP11HTTP_BINDING, getUriString());

            Dispatch<Source> dispatch = svc.createDispatch(portQName, Source.class, Service.Mode.PAYLOAD);
            
            // I should be able to send no value here, but the dispatch throws an exception
            // if soapAction == null.  We allow the default "" to get sent in that case.
            // In SOAP 1.1 we must send a SoapAction.
            String soapAction = (String)this.executionInfo.getOtherProperties().get("SOAPAction");
            if(null != soapAction) {
            	dispatch.getRequestContext().put(Dispatch.SOAPACTION_URI_PROPERTY, soapAction);
            }
            
            String requestDocument = xmlOutputter.outputString(this.requestDoc);
            
        	this.config.getLogger().logDetail(requestDocument);
            
            StringReader reader = new StringReader(requestDocument);
            Source input = new StreamSource(reader);
            // Invoke the operation.
            Source output = dispatch.invoke(input);
            return createSQLXML(output);
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
	}
	
	private SQLXML createSQLXML(Source output) {
		return (SQLXML)this.config.getTypeFacility().convertToRuntimeType(output);
	}	



	public int getDocumentCount() throws ConnectorException {
		return 1;
	}
	
	public final static String soapNSLabel = "SOAP-ENV"; //$NON-NLS-1$
	public static final String soapHeader= "Header"; //$NON-NLS-1$
	public static final String DUMMY_NS_PREFIX = "mm-dummy";  //$NON-NLS-1$	
	public static final String DUMMY_NS_NAME = "http://www.metamatrix.com/dummy";  //$NON-NLS-1$
	
	
	private Document setRequests(List<CriteriaDesc> params) throws ConnectorException {
		List<CriteriaDesc[]> requestPerms = RequestGenerator.getRequests(params);
		CriteriaDesc[] queryParameters = requestPerms.get(0);
		
		List<CriteriaDesc> newList = java.util.Arrays.asList(queryParameters);
		List<CriteriaDesc> queryList = new ArrayList<CriteriaDesc>(newList);

		List<CriteriaDesc> headerParams = new ArrayList<CriteriaDesc>();
		List<CriteriaDesc> bodyParams = new ArrayList<CriteriaDesc>();
		sortParams(queryList, headerParams, bodyParams);

		String namespacePrefixes = this.executionInfo.getOtherProperties().get(Constants.NAMESPACE_PREFIX_PROPERTY_NAME);
		String inputParmsXPath = this.executionInfo.getOtherProperties().get(DocumentBuilder.PARM_INPUT_XPATH_TABLE_PROPERTY_NAME); 
		return createXMLRequestDoc(bodyParams, namespacePrefixes, inputParmsXPath);
	}
	
    private void sortParams(List<CriteriaDesc> allParams, List<CriteriaDesc> headerParams, List<CriteriaDesc> bodyParams) throws ConnectorException {
		// sort the parameter list into header and body content
		// replace this later with model extensions
		for (CriteriaDesc desc : allParams) {
			if (desc.getInputXpath().startsWith(soapNSLabel+ ":" + soapHeader)) { //$NON-NLS-1$
				headerParams.add(desc);
			} else {
				bodyParams.add(desc);
			}
		}
	}
    
	private Document createXMLRequestDoc(List params, String namespacePrefixes, String inputParmsXPath)
			throws ConnectorException {
		Document doc;
		DocumentBuilder builder = new DocumentBuilder();
		//TODO: always set to encoded false - rareddy
		builder.setUseTypeAttributes(false);
		final String slash = "/";
		final String dotSlash = "./";
		boolean hasDummy = false;
		if (inputParmsXPath.equals(dotSlash) || inputParmsXPath.equals(slash) || inputParmsXPath.equals("")) {
			inputParmsXPath = DUMMY_NS_PREFIX + ":dummy";
			namespacePrefixes = namespacePrefixes + " xmlns:" + DUMMY_NS_PREFIX + "=\"" + DUMMY_NS_NAME + "\"";
			hasDummy = true;
		}
		doc = builder.buildDocument(params, inputParmsXPath, namespacePrefixes);
		if (hasDummy) {
			// Since there is no real root - these should all be elements
			Element element = (Element) doc.getRootElement().getChildren().get(0);
			element.detach();
			doc = new Document(element);
		}
		return doc;
	}    
}
