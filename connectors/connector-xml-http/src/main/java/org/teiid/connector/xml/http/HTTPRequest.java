package org.teiid.connector.xml.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.util.ParameterParser;
import org.jdom.Document;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ExecutionContext;

import com.metamatrix.connector.xml.Constants;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.DocumentBuilder;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.OutputXPathDesc;
import com.metamatrix.connector.xml.base.ParameterDescriptor;
import com.metamatrix.connector.xml.base.RequestGenerator;
import com.metamatrix.connector.xml.streaming.DocumentImpl;
import com.metamatrix.core.util.Assertion;

public class HTTPRequest extends BaseRequest {
	
	private HTTPRequestor requestor;

    private HttpMethod request;

    private boolean m_allowHttp500;

    public static final String PARM_INPUT_XPATH_TABLE_PROPERTY_NAME = "XPathRootForInput"; //$NON-NLS-1$

    public static final String PARM_INPUT_NAMESPACE_TABLE_PROPERTY_NAME = "NamespaceForDocument"; //$NON-NLS-1$
    
    protected SQLXML response;
    
    protected List<CriteriaDesc> parameters;
    protected HTTPManagedConnectionFactory config;
    protected ExecutionContext context;
    
	public HTTPRequest(HTTPConnectorState state, ExecutionInfo exeInfo, List<CriteriaDesc> parameters, HTTPManagedConnectionFactory config, ExecutionContext context) throws ConnectorException {
		super(state, exeInfo);
		this.parameters = parameters;
		this.config = config;
		this.context = context;
		
		processOutputXPathDescs(exeInfo.getRequestedColumns(), parameters);
    	initialize();
    	createRequests();
	}

	protected void initialize() throws ConnectorException {
		requestor = new HTTPRequestor(state.getLogger(), state.getAccessMethod());
		setAllowHttp500(false);
		try {
			String uri = buildRawUriString();
			requestor.validateURL(uri);
		} catch (IOException ioe) {
			throw new ConnectorException(ioe);
		}
		state.getLogger().logDetail(Messages.getString("HTTPExecutor.url.validated")); //$NON-NLS-1$
	}
	
	public com.metamatrix.connector.xml.Document getDocumentStream() throws ConnectorException {
		com.metamatrix.connector.xml.Document document;
		//ExecutionContext exeContext = execution.getExeContext();

        // Is this a request part joining across a document
        CriteriaDesc criterion = this.exeInfo.getResponseIDCriterion();
        if (criterion != null) {
            String responseid = (String) (criterion.getValues().get(0));
            SQLXML xml = this.config.getResponse(responseid);
            Assertion.isNotNull(xml);
        	document = new DocumentImpl(xml, responseid);
        } else {
        	// Not a join, but might still be cached.
        	// Not cached, so make the request
            SQLXML responseBody = executeRequest();
    		this.config.setResponse(this.context.getExecutionCountIdentifier(), responseBody);
            this.response = responseBody;
            //InputStream filteredStream = getState().addStreamFilters(responseBody);
            document = new DocumentImpl(responseBody, this.context.getExecutionCountIdentifier());
        }
		return document;
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

    private void setAllowHttp500(boolean allowHttp500) {
        m_allowHttp500 = allowHttp500;
    }

    public boolean getAllowHttp500() {
        return m_allowHttp500;
    }
    
	public void release() {
		if (request != null) {
			request.releaseConnection();
		}
		if (response != null) {
			this.config.removeResponse(this.context.getExecutionCountIdentifier());
			try {
				response.free();
			} catch (SQLException e) {
			}
			response = null;
		}
	}

    protected SQLXML executeRequest() throws ConnectorException {
        HttpClient client = (getState()).getClient();
        modifyRequest(client, request);
        InputStream responseBody = requestor.fetchXMLDocument(client,request, getAllowHttp500());
    	return createSQLXML(new StreamSource(responseBody));
    }
    
	private SQLXML createSQLXML(Source output) {
		return (SQLXML)this.config.getTypeFacility().convertToRuntimeType(output);
	}

    private void createRequests() throws ConnectorException {
        if (checkIfRequestIsNeeded(exeInfo)) {
            setRequests(this.parameters, getUriString());
        }
    }

    protected void modifyRequest(HttpClient client, HttpMethod method) throws ConnectorException {
    	if(this.config.getUseHttpBasic() && this.config.getHttpBasicAuthUserName() != null) {
			AuthScope authScope = new AuthScope(null, -1); // Create AuthScope for any host (null) and any port (-1).
    		Credentials defCred = new UsernamePasswordCredentials(this.config.getHttpBasicAuthUserName(), this.config.getHttpBasicAuthPassword());
			client.getState().setCredentials(authScope, defCred);
		} else {
			throw new ConnectorException(org.teiid.connector.xml.http.Messages.getString("HTTPExecutor.bad.security.configuration"));
		}
    }

    public boolean cannotProjectParameter(CriteriaDesc parmCriteria) {
        return parmCriteria.getNumberOfValues() > 1
                && parmCriteria.isUnlimited()
                && !((getState()).getParameterMethod() == HTTPConnectorState.PARAMETER_NAME_VALUE);
    }

    // this routine builds the input XML document using JDOM structures
    protected Document buildInputXMLDocument(List parameters, String inputParmsXPath) throws ConnectorException {
        String namespacePrefixes = exeInfo.getOtherProperties().get(Constants.NAMESPACE_PREFIX_PROPERTY_NAME);
        DocumentBuilder builder = new DocumentBuilder();
        return builder.buildDocument(parameters, inputParmsXPath,namespacePrefixes);
    }

    protected NameValuePair[] createNameValuePairs(List paramPairs) throws ConnectorException {
        NameValuePair[] pairs = new NameValuePair[paramPairs.size()];
        for (int i = 0; i < pairs.length; i++) {
            CriteriaDesc cd = (CriteriaDesc) paramPairs.get(i);
            String name = (cd.getInputXpath() == null || cd.getInputXpath().length() == 0) ? cd.getColumnName() : cd.getInputXpath();
            NameValuePair pair = new NameValuePair(name, cd.getCurrentIndexValue());
            pairs[i] = pair;
        }

        return pairs;
    }

    protected void setRequests(List<CriteriaDesc> params, String bindingURIValue)
            throws ConnectorException {

        String xmlDoc = null;
                // see if there are parameters to set
        if (getState().getParameterMethod() == HTTPConnectorState.PARAMETER_NONE || params.size() == 0) {
            getLogger().logTrace("XML Connector Framework: no parameters for request"); //$NON-NLS-1$
        }
        List<CriteriaDesc[]> requestPerms = RequestGenerator.getRequests(params);

        for (CriteriaDesc[] queryParameters: requestPerms) {
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

            java.util.List newList = java.util.Arrays.asList(queryParameters);
            List queryList = new ArrayList(newList);

            String parameterMethod = getState().getParameterMethod();
            if (parameterMethod == HTTPConnectorState.PARAMETER_XML_REQUEST ||
                    parameterMethod == HTTPConnectorState.PARAMETER_XML_QUERY_STRING ) {
                xmlDoc = createXMLRequestDocString(queryList);
                String paramName = getState().getXmlParameterName();
                if(null != paramName) {
                    pairs = new NameValuePair[] { new NameValuePair(getState().getXmlParameterName(), xmlDoc) };
                    if (pairs != null) {
                        attemptConditionalLog("XML Connector Framework: request parameters -\n " + generatePairString(pairs)); 
                    }
                }
            } else if (parameterMethod == HTTPConnectorState.PARAMETER_NAME_VALUE || parameterMethod == HTTPConnectorState.PARAMETER_NONE) {
                pairs = createNameValuePairs(queryList);
            }

            HttpMethod method = null;
            String accessMethod = getState().getAccessMethod();
            if(accessMethod.equals(HTTPConnectorState.POST)) {
                method = requestor.generateMethod(bindingURI);
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
                            method = requestor.generateMethod(fullQueryString);
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
                method = requestor.generateMethod(bindingURI);
                if (pairs == null) {
                    throw new ConnectorException(
                            org.teiid.connector.xml.http.Messages
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

    protected void addGetValues(NameValuePair[] pairs, HttpMethod method) {
        method.setQueryString(pairs);
    }

    protected void addPostValues(NameValuePair[] pairs, PostMethod method) {
        method.addParameters(pairs);
    }

    protected Document createXMLRequestDoc(List parameterPairs) throws ConnectorException {
        Map<String, String> props = exeInfo.getOtherProperties();
        String inputParmsXPath = props.get(PARM_INPUT_XPATH_TABLE_PROPERTY_NAME);
        Document inputXMLDoc = buildInputXMLDocument(parameterPairs,inputParmsXPath);
        return inputXMLDoc;
    }

    protected String createXMLRequestDocString(List parameterPairs) throws ConnectorException {
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
                    org.teiid.connector.xml.http.Messages
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
                            org.teiid.connector.xml.http.Messages
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

	protected HTTPConnectorState getState() {
		return state;
	}

	protected ConnectorLogger getLogger() {
		return getState().getLogger();
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
	 * Put the input parameter value in the result column value if possible.
	 */
	private void setOutputValues(final List parameterPairs, OutputXPathDesc xPath) throws ConnectorException {
	    int colNum = xPath.getColumnNumber();
	    for (int x = 0; x < parameterPairs.size(); x++) {
	        CriteriaDesc parmCriteria = (CriteriaDesc) parameterPairs.get(x);
	        if (parmCriteria.getColumnNumber() == colNum) {
	            if (cannotProjectParameter(parmCriteria)) {
	                throw new ConnectorException(Messages.getString("HTTPExecutor.cannot.project.repeating.values")); //$NON-NLS-1$
	            } 
                xPath.setCurrentValue(parmCriteria.getCurrentIndexValue());
                break;
	        }
	    }        
	}





}
