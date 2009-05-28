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


package com.metamatrix.connector.xml.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.util.ParameterParser;
import org.jdom.Document;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ExecutionContext;

import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.Constants;
import com.metamatrix.connector.xml.ResultProducer;
import com.metamatrix.connector.xml.SAXFilterProvider;
import com.metamatrix.connector.xml.XMLConnection;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.DocumentBuilder;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.LoggingInputStreamFilter;
import com.metamatrix.connector.xml.base.OutputXPathDesc;
import com.metamatrix.connector.xml.base.ParameterDescriptor;
import com.metamatrix.connector.xml.base.RequestGenerator;
import com.metamatrix.connector.xml.cache.CachedXMLStream;
import com.metamatrix.connector.xml.streaming.DocumentImpl;

public class HTTPExecutor implements ResultProducer {

    protected XMLExecution execution;
	
	protected HTTPRequestor m_requestor;

    protected HttpMethod request;

    private HTTPConnectorState state;

    private boolean m_allowHttp500;

	private ExecutionInfo exeInfo;

    public static final String PARM_INPUT_XPATH_TABLE_PROPERTY_NAME = "XPathRootForInput"; //$NON-NLS-1$

    public static final String PARM_INPUT_NAMESPACE_TABLE_PROPERTY_NAME = "NamespaceForDocument"; //$NON-NLS-1$

    /**
     * @param state
     * @param execution
     * @param exeInfo 
     * @throws ConnectorException
     */
    public HTTPExecutor(XMLConnectorState state, XMLExecution execution, ExecutionInfo exeInfo)
            throws ConnectorException {
        this.execution = execution;
    	this.setExeInfo(exeInfo);
        this.setState((HTTPConnectorState) state);
        processOutputXPathDescs(getExeInfo().getRequestedColumns()
        		, getExeInfo().getParameters());
        m_requestor = new HTTPRequestor(getLogger(), getState().getAccessMethod());
        setAllowHttp500(false);
        try {
            String uri = buildRawUriString();
            m_requestor.validateURL(uri);
        } catch (IOException ioe) {
            throw new ConnectorException(ioe);
        }
        getLogger()
                .logDetail(Messages.getString("HTTPExecutor.url.validated")); //$NON-NLS-1$
    }

    protected void releaseDocumentStream() {
        if (request != null) {
            request.releaseConnection();
            request = null;
        }
    }

    protected String getCacheKey() throws ConnectorException {

        if (request == null) {
            String message = com.metamatrix.connector.xml.http.Messages
                    .getString("HttpExecutor.cannot.create.cachekey");
            throw new ConnectorException(message);
        }
        // the key consists of a String in the form of
        // |uri|parameterList|
        String userName = execution.getConnection().getUser();
        String session = execution.getConnection().getQueryId();

        StringBuffer cacheKey = new StringBuffer();
        cacheKey.append("|"); //$NON-NLS-1$
        cacheKey.append(userName);
        cacheKey.append("|");
        cacheKey.append(session);
        cacheKey.append("|");
        cacheKey.append(buildUriString());
        cacheKey.append("|"); //$NON-NLS-1$

        if (request instanceof PostMethod) {
            NameValuePair[] pairs = ((PostMethod) request).getParameters();
            if (pairs == null || pairs.length == 0) {
                if (((PostMethod) request).getRequestEntity() != null) {
                    String requestBodyAsString = ((StringRequestEntity) (((PostMethod) request)
                            .getRequestEntity())).getContent();
                    cacheKey.append(requestBodyAsString);
                }
            } else {
                cacheKey.append(generatePairString(pairs));
            }
        } else {
            cacheKey.append(request.getQueryString());
        }
        return cacheKey.toString();
    }

    protected InputStream getDocumentStream() throws ConnectorException {
        HttpClient client = (getState()).getClient();
        XMLConnection conn = execution.getConnection();
        try {
            HTTPTrustDeserializer ser = (HTTPTrustDeserializer) conn
                    .getTrustedPayloadHandler();
            ser.modifyRequest(client, request);
        } catch (ClassCastException cce) {
            throw new ConnectorException(
                    com.metamatrix.connector.xml.http.Messages
                            .getString("HTTPExecutor.class.not.instance.of.HTTPTrustDeserializer"));
        } catch (Exception e) {
            ConnectorException ce = new ConnectorException(
                    com.metamatrix.connector.xml.http.Messages
                            .getString("HTTPExecutor.unable.to.create.trust.deserializer"));
            ce.setStackTrace(e.getStackTrace());
            throw ce;
        }
        modifyRequest(client, request);
        InputStream responseBody = m_requestor.fetchXMLDocument(client,
                request, getAllowHttp500());
        return responseBody;
    }

	@Override
    public Iterator<com.metamatrix.connector.xml.Document> getXMLDocuments() throws ConnectorException {
		ArrayList<com.metamatrix.connector.xml.Document> result = new ArrayList<com.metamatrix.connector.xml.Document>();
		createRequests();
        CachingConnector connector = execution.getConnection().getConnector();
        ExecutionContext exeContext = execution.getExeContext();
        String cacheKey = getCacheKey();
        
        
        // Is this a request part joining across a document
        CriteriaDesc criterion = getExeInfo().getResponseIDCriterion();
        if (null != criterion) {
            String responseid = (String) (criterion.getValues().get(0));
            
            if(null == exeContext.get(responseid)) {
            	throw new ConnectorException(Messages.getString("HTTPExecutor.No.doc.in.cache"));
            } else {
            	InputStream stream = new CachedXMLStream(exeContext, responseid);
            	com.metamatrix.connector.xml.Document doc = new DocumentImpl(stream, cacheKey);
            	result.add(doc);
            }
        }

        // Not a join, but might still be cached.
            if (null == exeContext.get(cacheKey)) {
                SAXFilterProvider provider = null;
                provider = getState().getSAXFilterProvider();
                InputStream responseBody = getDocumentStream();
                InputStream filteredStream = addStreamFilters(responseBody, getLogger());
                com.metamatrix.connector.xml.Document doc = new DocumentImpl(filteredStream, cacheKey);
                result.add(doc);
        }
        return result.iterator();
	}

    private void createRequests() throws ConnectorException {
        if (checkIfRequestIsNeeded(getExeInfo())) {
            String uriString = buildUriString();
            setRequests(getExeInfo().getParameters(), uriString);
        }
    }

    protected void modifyRequest(HttpClient client, HttpMethod method)
            throws ConnectorException {
        // to be overridden by subclasses
    }

    public boolean cannotProjectParameter(CriteriaDesc parmCriteria) {
        return parmCriteria.getNumberOfValues() > 1
                && parmCriteria.isUnlimited()
                && !((getState()).getParameterMethod() == HTTPConnectorState.PARAMETER_NAME_VALUE);
    }

    // this routine builds the input XML document using JDOM structures
    protected Document buildInputXMLDocument(List parameters,
            String inputParmsXPath) throws ConnectorException {
        String namespacePrefixes = getExeInfo().getOtherProperties()
                .getProperty(Constants.NAMESPACE_PREFIX_PROPERTY_NAME);
        DocumentBuilder builder = new DocumentBuilder();
        return builder.buildDocument(parameters, inputParmsXPath,
                namespacePrefixes);
    }

    protected NameValuePair[] createNameValuePairs(List paramPairs)
            throws ConnectorException {
        NameValuePair[] pairs = new NameValuePair[paramPairs.size()];
        for (int i = 0; i < pairs.length; i++) {
            CriteriaDesc cd = (CriteriaDesc) paramPairs.get(i);
            String name = (cd.getInputXpath() == null || cd.getInputXpath()
                    .length() == 0) ? cd.getColumnName() : cd.getInputXpath();
            NameValuePair pair = new NameValuePair(name, cd
                    .getCurrentIndexValue());
            pairs[i] = pair;
        }

        return pairs;
    }

    protected void setRequests(List params, String bindingURIValue)
            throws ConnectorException {

        String xmlDoc = null;
                // see if there are parameters to set
        if (getState().getParameterMethod() == HTTPConnectorState.PARAMETER_NONE
                || params.size() == 0) {
            getLogger()
                    .logTrace("XML Connector Framework: no parameters for request"); //$NON-NLS-1$
        }
        // mrh: Originally HTTP and SOAP did the creation of multiple requests
        // from IN criteria
        // themselves, but that has been moved to the base code. So
        // XMLExecutionImpl.getRequestPerms
        // will now always return just one permutation. However, 'requestPerms'
        // is a different
        // structure than 'params' so I won't refactor that just yet.
        List requestPerms = RequestGenerator.getRequestPerms(params);

        for (int i = 0; i < requestPerms.size(); i++) {
            NameValuePair[] pairs = null;
            String bindingURI = null;
            String bindingQueryString = null;
            if(-1 != bindingURIValue.indexOf('?')) {
                bindingURI = bindingURIValue.substring(1, bindingURIValue.indexOf('?'));
                bindingQueryString = bindingURIValue.substring(bindingURIValue.indexOf('?') + 1, bindingURIValue.length() -1);
            } else {
            	bindingURI = bindingURIValue;
            }

            if(-1 != bindingURI.indexOf("<") || -1 != bindingURI.indexOf("<")) {
                bindingURI = removeAngleBrackets(bindingURI);
            }

            CriteriaDesc[] queryParameters = (CriteriaDesc[]) requestPerms
                    .get(i);
            java.util.List newList = java.util.Arrays.asList(queryParameters);
            List queryList = new ArrayList(newList);

            String parameterMethod = getState().getParameterMethod();
            if (parameterMethod == HTTPConnectorState.PARAMETER_XML_REQUEST ||
                    parameterMethod == HTTPConnectorState.PARAMETER_XML_QUERY_STRING ) {
                xmlDoc = createXMLRequestDocString(queryList);
                String paramName = getState().getXmlParameterName();
                if(null != paramName) {
                    pairs = new NameValuePair[] { new NameValuePair(getState()
                            .getXmlParameterName(), xmlDoc) };
                    if (pairs != null) {
                        attemptConditionalLog("XML Connector Framework: request parameters -\n "
                                + generatePairString(pairs)); 
                    }
                }
            } else if (parameterMethod == HTTPConnectorState.PARAMETER_NAME_VALUE ||
            		parameterMethod == HTTPConnectorState.PARAMETER_NONE) {
                pairs = createNameValuePairs(queryList);
            }

            HttpMethod method = null;
            String accessMethod = getState().getAccessMethod();
            if(accessMethod.equals(HTTPConnectorState.POST)) {
                method = m_requestor.generateMethod(bindingURI);
                PostMethod post = (PostMethod) method;
                if (pairs == null) {
                    //POST-DOC-NO_PARAM_NAME
                    if(bindingQueryString != null) {
                        pairs = getPairsFromQueryString(xmlDoc, bindingQueryString);
                        post.addParameters(pairs);
                        attemptConditionalLog("XML Connector Framework: request parameters -\n " + generatePairString(pairs)); //$NON-NLS-1$
                    } else {
                        post.setRequestEntity(new StringRequestEntity(xmlDoc));
                        attemptConditionalLog("XML Connector Framework: request body set to: " + xmlDoc); //$NON-NLS-1$
                    }
                } else {
                    //POST-DOC-WITH_PARAM_NAME
                    if(parameterMethod == HTTPConnectorState.PARAMETER_XML_QUERY_STRING) {
                        //QUERY_STRING
                        StringBuffer requestEntity = new StringBuffer();
                        if(null != bindingQueryString) {
                            requestEntity.append(bindingQueryString);
                            requestEntity.append('&');
                        }
                        requestEntity.append(getState().getXmlParameterName());
                        requestEntity.append('=');
                        requestEntity.append(xmlDoc);
                        URI realURI = null;
                        try {
                            realURI = new URI(bindingURI + "?" + URLEncoder.encode(requestEntity.toString()));
                            String fullQueryString = realURI.toString();
                            method = m_requestor.generateMethod(fullQueryString);
                            attemptConditionalLog("XML Connector Framework: request set to -\n " + fullQueryString); //$NON-NLS-1$
                        } catch (URISyntaxException e) {
                            throw new ConnectorException(e.getMessage());
                        }
                    } else {
                        //XML_REQUEST
                        if(null != bindingQueryString){
                            NameValuePair[] bindingPairs = getPairsFromQueryString(xmlDoc, bindingQueryString);
                            pairs = combinePairArrays(pairs, bindingPairs);
                        }
                        post.addParameters(pairs);
                        attemptConditionalLog("XML Connector Framework: request parameters -\n " + generatePairString(pairs)); //$NON-NLS-1$
                    }
                }
            } else if (accessMethod.equals(HTTPConnectorState.GET)){
                method = m_requestor.generateMethod(bindingURI);
                if (pairs == null) {
                    throw new ConnectorException(
                            com.metamatrix.connector.xml.http.Messages
                                    .getString("HTTPExecutor.parameter.name.required.for.get")); //$NON-NLS-1$
                }
                if(null != bindingQueryString){
                    NameValuePair[] bindingPairs = getPairsFromQueryString(xmlDoc, bindingQueryString);
                    pairs = combinePairArrays(pairs, bindingPairs);
                }
                addGetValues(pairs, method);
                attemptConditionalLog("XML Connector Framework: request paramters -\n " + generatePairString(pairs)); //$NON-NLS-1$

            }
            request = method;
            getLogger().logInfo("XML Connector Framework: request created"); //$NON-NLS-1$
        }
    }

    protected void addGetValues(NameValuePair[] pairs, HttpMethod method)
            throws ConnectorException {
        method.setQueryString(pairs);
    }

    protected void addPostValues(NameValuePair[] pairs, PostMethod method)
            throws ConnectorException {
        method.addParameters(pairs);
    }

    protected Document createXMLRequestDoc(List parameterPairs)
            throws ConnectorException {
        Properties props = getExeInfo().getOtherProperties();
        String inputParmsXPath = props
                .getProperty(PARM_INPUT_XPATH_TABLE_PROPERTY_NAME);
        Document inputXMLDoc = buildInputXMLDocument(parameterPairs,
                inputParmsXPath);
        return inputXMLDoc;
    }

    protected String createXMLRequestDocString(List parameterPairs)
            throws ConnectorException {
        Document inputXMLDoc = createXMLRequestDoc(parameterPairs);
        String xmlStr = HTTPRequestor.outputStringFromDoc(inputXMLDoc);
        return xmlStr;
    }

    private String generatePairString(NameValuePair[] pairs) {
        StringBuffer pairString = new StringBuffer();
        for (int j = 0; j < pairs.length; j++) {
            if (j > 0) {
                pairString.append("&"); //$NON-NLS-1$
            }
            pairString.append(pairs[j].getName());
            pairString.append("="); //$NON-NLS-1$
            pairString.append(pairs[j].getValue());
        }
        return pairString.toString();
    }

    private NameValuePair[] combinePairArrays(NameValuePair[] pairs, NameValuePair[] bindingPairs) {
        NameValuePair[] allPairs = new NameValuePair[bindingPairs.length + pairs.length];
        System.arraycopy(bindingPairs, 0, allPairs, 0, bindingPairs.length);
        System.arraycopy(pairs, 0, allPairs, bindingPairs.length, pairs.length);
        return allPairs;
    }

    private NameValuePair[] getPairsFromQueryString(String xmlDoc, String bindingQueryString) {
        NameValuePair[] pairs;
        ParameterParser parser = new ParameterParser();
        List bindingQueryParams = parser.parse(bindingQueryString, '=');
        bindingQueryParams.add(new NameValuePair(null, xmlDoc));
        pairs = (NameValuePair[]) bindingQueryParams.toArray();
        return pairs;
    }

    protected NameValuePair[] generatePairs(String pairString) {
        NameValuePair[] pairs;
        int numPairs = 1;
        String dummy = pairString;
        while (dummy.indexOf('&') >= 0) {
            ++numPairs;
            dummy = dummy.substring(dummy.indexOf('&'));
        }
        pairs = new NameValuePair[numPairs];
        // reset dummy in case its been substring'ed
        dummy = pairString;
        int ctr = 0;
        if (numPairs > 1) {
            while (dummy.indexOf('&') >= 0) {
                String name = dummy.substring(0, dummy.indexOf('='));
                String value = dummy.substring(dummy.indexOf('='), dummy
                        .indexOf('&'));
                pairs[ctr] = new NameValuePair(name, value);
                ++ctr;
            }
            // last one
            String name = dummy.substring(0, dummy.indexOf('='));
            String value = dummy.substring(dummy.indexOf('='), dummy
                    .indexOf('&'));
            pairs[ctr] = new NameValuePair(name, value);
        }
        return pairs;
    }

    public Serializable getRequestObject(int i) throws ConnectorException {
        HttpMethod method = request;
        HttpInfo newInfo = new HttpInfo();
        newInfo.m_distinguishingId = i;
        try {
            newInfo.m_uri = method.getURI().getEscapedURI();
        } catch (URIException urie) {
            ConnectorException ce = new ConnectorException(
                    com.metamatrix.connector.xml.http.Messages
                            .getString("HTTPExecutor.unable.to.recreate.uri")); //$NON-NLS-1$
            ce.setStackTrace(urie.getStackTrace());
            throw ce;
        }
        if (method instanceof GetMethod) {
            newInfo.m_request = method.getQueryString();
            newInfo.m_method = HTTPConnectorState.GET;
            newInfo.m_paramMethod = null;
        } else {
            newInfo.m_method = HTTPConnectorState.POST;
            NameValuePair[] pairs = ((PostMethod) method).getParameters();
            if (pairs == null) {
                newInfo.m_paramMethod = HttpInfo.RESPONSEBODY;
                if ((newInfo.m_request = ((StringRequestEntity) (((PostMethod) method)
                        .getRequestEntity())).getContent()) == null) {
                    ConnectorException ce = new ConnectorException(
                            com.metamatrix.connector.xml.http.Messages
                                    .getString("HTTPExecutor.unable.to.recreate.request")); //$NON-NLS-1$
                    throw ce;
                }
            } else {
                newInfo.m_paramMethod = HttpInfo.NAMEVALUE;
                newInfo.m_request = generatePairString(pairs);
            }
        }
        return newInfo;
    }

    private void setAllowHttp500(boolean allowHttp500) {
        m_allowHttp500 = allowHttp500;
    }

    public boolean getAllowHttp500() {
        return m_allowHttp500;
    }

	@Override
	public void closeStreams() {
		releaseDocumentStream();
	}

	private void setExeInfo(ExecutionInfo exeInfo) {
		this.exeInfo = exeInfo;
	}

	protected ExecutionInfo getExeInfo() {
		return exeInfo;
	}
	
	private void setState(HTTPConnectorState state) {
		this.state = state;
	}

	protected HTTPConnectorState getState() {
		return state;
	}

	protected String buildUriString() {
	    String uriString = "<" + buildRawUriString() + ">"; //$NON-NLS-1$
	    getLogger().logDetail("XML Connector Framework: using url " + uriString); //$NON-NLS-1$
	    return uriString;
	}

	protected ConnectorLogger getLogger() {
		return getState().getLogger();
	}

	protected String buildRawUriString() {
	    String location = getExeInfo().getLocation();
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
	
	    if (location == null) {
	        final String tableServletCallPathProp = "ServletCallPathforURL"; //$NON-NLS-1$
	        location = getExeInfo().getOtherProperties().getProperty(
	                tableServletCallPathProp);
	    }
	
	    String retval = (getState()).getUri();
	    if (location != null && location.trim().length() > 0) {
	        retval = retval + "/" + location; //$NON-NLS-1$
	    }
	    return retval;
	}

	public InputStream addStreamFilters(InputStream response, ConnectorLogger logger)
	throws ConnectorException {
		
		if(getState().isLogRequestResponse()) {
			response = new LoggingInputStreamFilter(response, logger);
		}
		
		InputStream filter = null;
		try {
			Class pluggableFilter = Thread.currentThread().getContextClassLoader().loadClass(getState().getPluggableInputStreamFilterClass());
			Constructor ctor = pluggableFilter.getConstructor(
					new Class[] { java.io.InputStream.class, org.teiid.connector.api.ConnectorLogger.class});
			filter = (InputStream) ctor.newInstance(new Object[] {response, logger});
		} catch (Exception cnf) {
			throw new ConnectorException(cnf);
		}
		return filter;
	}

	/**
	 * Examines the Query to determine if a request to a source is needed.  If any of the 
	 * request parameters is a ResponseIn, then we don't need to make a request because it 
	 * has already been made by another call to Execution.execute()
	 */ 
    public static boolean checkIfRequestIsNeeded(ExecutionInfo info) throws ConnectorException {
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

	protected String removeAngleBrackets(String uri) {
	    char[] result = new char[uri.length()];
	    char[] source = uri.toCharArray();
	    int r = 0;
	    char v;
	    for(int i = 0; i < source.length; i++) {
	          v = source[i];
	          if(v == '<' || v == '>')
	             continue;
	          result[r] = v;
	          ++r;
	    }
	    return new String(result).trim();
	}

	protected void attemptConditionalLog(String message) {
	    if (getState().isLogRequestResponse()) {
	        getLogger().logInfo(message);
	    }
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
	            setOutputValues(parameterPairs, xPath);
	        }
	    }
	}

	/**
	 * Put the input parameter value in the result column value if possible.
	 */
	private void setOutputValues(final List parameterPairs, OutputXPathDesc xPath) throws ConnectorException {
	    int colNum = xPath.getColumnNumber();
	    for (int x = 0; x < parameterPairs.size(); x++) {
	        CriteriaDesc parmCriteria =
	                    (CriteriaDesc) parameterPairs.get(x);
	        if (parmCriteria.getColumnNumber() == colNum) {
	            if (cannotProjectParameter(parmCriteria)) {
	                throw new ConnectorException(Messages.getString("HTTPExecutor.cannot.project.repeating.values")); //$NON-NLS-1$
	            } else {
	                xPath.setCurrentValue(parmCriteria.getCurrentIndexValue());
	                break;
	            }
	        }
	    }        
	}


}
