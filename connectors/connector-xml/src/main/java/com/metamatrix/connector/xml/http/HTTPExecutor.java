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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
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

import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.SAXFilterProvider;
import com.metamatrix.connector.xml.XMLConnection;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.DocumentBuilder;
import com.metamatrix.connector.xml.base.DocumentInfo;
import com.metamatrix.connector.xml.base.RequestGenerator;
import com.metamatrix.connector.xml.base.RequestResponseDocumentProducer;
import com.metamatrix.connector.xml.base.Response;
import com.metamatrix.connector.xml.base.XMLDocument;
import com.metamatrix.connector.xml.cache.DocumentCache;
import com.metamatrix.connector.xml.cache.IDocumentCache;

/**
 * created by JChoate on Jun 27, 2005
 *
 */
public class HTTPExecutor extends RequestResponseDocumentProducer {

    protected HTTPRequestor m_requestor;

    protected HttpMethod request;

    private HTTPConnectorState h_state;

    private boolean m_allowHttp500;

    public static final String PARM_INPUT_XPATH_TABLE_PROPERTY_NAME = "XPathRootForInput"; //$NON-NLS-1$

    public static final String PARM_INPUT_NAMESPACE_TABLE_PROPERTY_NAME = "NamespaceForDocument"; //$NON-NLS-1$

    /**
     * @param state
     * @param execution
     * @throws ConnectorException
     */
    public HTTPExecutor(XMLConnectorState state, XMLExecution execution)
            throws ConnectorException {
        super(state, execution);
        h_state = (HTTPConnectorState) state;
        m_requestor = new HTTPRequestor(getLogger(), h_state.getAccessMethod());
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

    public void releaseDocumentStream(int i) throws ConnectorException {
        if (request != null) {
            request.releaseConnection();
            request = null;
        }
    }

    public int getDocumentCount() throws ConnectorException {
        return 1;
    }

    public String getCacheKey(int i) throws ConnectorException {

        // http method, xmlparamname and parameter method are static for a given
        // binding so they are not needed in a unique key

        if (request == null) {
            String message = com.metamatrix.connector.xml.http.Messages
                    .getString("HttpExecutor.cannot.create.cachekey");
            throw new ConnectorException(message);
        }
        // the key consists of a String in the form of
        // |uri|parameterList|
        String userName = getExecution().getConnection().getUser();
        String session = getExecution().getConnection().getQueryId();

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

    public InputStream getDocumentStream(int i) throws ConnectorException {
        HttpClient client = ((HTTPConnectorState) getState()).getClient();
        XMLConnection conn = getExecution().getConnection();
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

    public Response getXMLResponse(int invocationNumber) throws ConnectorException {
        createRequests();
        CachingConnector connector = getExecution().getConnection().getConnector();
        IDocumentCache cache = getExecution().getCache();
        ExecutionContext exeContext = getExecution().getExeContext();
        String requestID = exeContext.getRequestIdentifier();
        String partID = exeContext.getPartIdentifier();
        String executionID = exeContext.getExecutionCountIdentifier();
        String cacheReference = requestID + partID + executionID + Integer.toString(invocationNumber);
        
        CriteriaDesc criterion = getExecutionInfo().getResponseIDCriterion();
        if (null != criterion) {
            String responseid = (String) (criterion.getValues().get(0));
            getExecution().getConnection().getConnector().createCacheObjectRecord(requestID, partID, executionID,
                  Integer.toString(invocationNumber), responseid);
            return new Response(responseid, this, cache, cacheReference);
        }

        int documentCount = getDocumentCount();
        String[] cacheKeys = new String[documentCount];
        XMLDocument[] docs = new XMLDocument[documentCount];
        for (int i = 0; i < documentCount; i++) {
            String cacheKey = getCacheKey(i);
            XMLDocument doc = DocumentCache.cacheLookup(cache, cacheKey, cacheReference);
            if (doc == null) {
                String documentDistinguishingId = "";
                if (documentCount > 1) {
                    documentDistinguishingId = new Integer(i).toString();
                }
                DocumentInfo info;
                SAXFilterProvider provider = null;
                provider = h_state.getSAXFilterProvider();
                InputStream responseBody = getDocumentStream(i);
                InputStream filteredStream = addStreamFilters(responseBody, getLogger());
                info = xmlExtractor.createDocumentFromStream(filteredStream,
                        documentDistinguishingId, provider);

                Document domDoc = info.m_domDoc;
                doc = new XMLDocument(domDoc, info.m_externalFiles);

                cache.addToCache(cacheKey, doc, info.m_memoryCacheSize, cacheReference);
                connector.createCacheObjectRecord(requestID, partID, executionID,
                      Integer.toString(invocationNumber), cacheKey);
            }
            docs[i] = doc;
            cacheKeys[i] = cacheKey;
        }
        return new Response(docs, cacheKeys, this, cache, cacheReference);
	}

    private void createRequests() throws ConnectorException {
        if (checkIfRequestIsNeeded(getExecutionInfo())) {
            String uriString = buildUriString();
            setRequests(getExecutionInfo().getParameters(), uriString);
        }
    }

    protected void modifyRequest(HttpClient client, HttpMethod method)
            throws ConnectorException {
        // to be overridden by subclasses
    }

    public boolean cannotProjectParameter(CriteriaDesc parmCriteria) {
        return parmCriteria.getNumberOfValues() > 1
                && parmCriteria.isUnlimited()
                && !(((HTTPConnectorState) getState()).getParameterMethod() == HTTPConnectorState.PARAMETER_NAME_VALUE);
    }

    // this routine builds the input XML document using JDOM structures
    protected Document buildInputXMLDocument(List parameters,
            String inputParmsXPath) throws ConnectorException {
        String namespacePrefixes = getExecutionInfo().getOtherProperties()
                .getProperty(NAMESPACE_PREFIX_PROPERTY_NAME);
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
        HTTPConnectorState state = (HTTPConnectorState) getState();

        // see if there are parameters to set
        if (state.getParameterMethod() == HTTPConnectorState.PARAMETER_NONE
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

            String parameterMethod = state.getParameterMethod();
            if (parameterMethod == HTTPConnectorState.PARAMETER_XML_REQUEST ||
                    parameterMethod == HTTPConnectorState.PARAMETER_XML_QUERY_STRING ) {
                xmlDoc = createXMLRequestDocString(queryList);
                String paramName = state.getXmlParameterName();
                if(null != paramName) {
                    pairs = new NameValuePair[] { new NameValuePair(state
                            .getXmlParameterName(), xmlDoc) };
                    if (pairs != null) {
                        attemptConditionalLog("XML Connector Framework: request parameters -\n "
                                + generatePairString(pairs)); //$NON-NLS-1$
                    }
                }
            } else if (parameterMethod == HTTPConnectorState.PARAMETER_NAME_VALUE ||
            		parameterMethod == HTTPConnectorState.PARAMETER_NONE) {
                pairs = createNameValuePairs(queryList);
            }

            HttpMethod method = null;
            String accessMethod = state.getAccessMethod();
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
                        requestEntity.append(state.getXmlParameterName());
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
        Properties props = getExecutionInfo().getOtherProperties();
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

    public void setAllowHttp500(boolean allowHttp500) {
        m_allowHttp500 = allowHttp500;
    }

    public boolean getAllowHttp500() {
        return m_allowHttp500;
    }
}
