package org.teiid.connector.xml.soap;

import java.io.StringReader;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.jdom.Document;
import org.jdom.output.XMLOutputter;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.xml.SOAPConnectorState;
import org.teiid.connector.xml.http.HTTPConnectorState;

import com.metamatrix.connector.xml.Constants;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.DocumentBuilder;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.RequestGenerator;

public class SOAPRequest extends org.teiid.connector.xml.http.HTTPRequest {

	/*SecurityToken secToken;*/
	XMLOutputter xmlOutputter = new XMLOutputter();
	Document doc;
	private int requestNumber;

	public SOAPRequest(SOAPConnectorState connectorState, SOAPManagedConnectionFactory config, ExecutionContext context, ExecutionInfo exeInfo, List<CriteriaDesc> parameters, int requestNumber)
			throws ConnectorException {
		super((HTTPConnectorState) connectorState, exeInfo, parameters, config, context);
		this.requestNumber = requestNumber;
		initialize();
	}
	
	protected void initialize() {		
	}
	
	public void release() {
	}
	
	@Override
	protected SQLXML executeRequest() throws ConnectorException {
		try {
			//TrustedPayloadHandler handler = execution.getConnection().getTrustedPayloadHandler();
			/*
			SecurityManagedConnectionFactory env = (SecurityManagedConnectionFactory)execution.getConnection().getConnectorEnv();
			secToken = SecurityToken.getSecurityToken(env, handler);
			*/
			
            QName svcQname = new QName("http://org.apache.cxf", "foo");
            QName portQName = new QName("http://org.apache.cxf", "bar");
            Service svc = Service.create(svcQname);
            svc.addPort(
                    portQName, 
                    SOAPBinding.SOAP11HTTP_BINDING,
                    removeAngleBrackets(getUriString()));

            Dispatch<Source> dispatch = svc.createDispatch(
                    portQName, 
                    Source.class, 
                    Service.Mode.PAYLOAD);
            
            // I should be able to send no value here, but the dispatch throws an exception
            // if soapAction == null.  We allow the default "" to get sent in that case.
            // In SOAP 1.1 we must send a SoapAction.
            String soapAction = (String)exeInfo.getOtherProperties().get("SOAPAction");
            if(null != soapAction) {
            	dispatch.getRequestContext().put(Dispatch.SOAPACTION_URI_PROPERTY, soapAction);
            }
            
            String requestDocument = xmlOutputter.outputString(doc);
            attemptConditionalLog(requestDocument);
            StringReader reader = new StringReader(requestDocument);
            Source input = new StreamSource(reader);
            // Invoke the operation.
            Source output = dispatch.invoke(input);
            response = createSQLXML(output);
            return response;
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
	}
	
	private SQLXML createSQLXML(Source output) {
		return (SQLXML)this.config.getTypeFacility().convertToRuntimeType(output);
	}	

	protected void setRequests(List<CriteriaDesc> params, String uriString)
			throws ConnectorException {
		
		List<CriteriaDesc[]> requestPerms = RequestGenerator.getRequests(params);
		CriteriaDesc[] queryParameters = requestPerms.get(0);
		
		List<CriteriaDesc> newList = java.util.Arrays.asList(queryParameters);
		List<CriteriaDesc> queryList = new ArrayList<CriteriaDesc>(newList);

		List<CriteriaDesc> headerParams = new ArrayList<CriteriaDesc>();
		List<CriteriaDesc> bodyParams = new ArrayList<CriteriaDesc>();
		sortParams(queryList, headerParams, bodyParams);

		String namespacePrefixes = exeInfo.getOtherProperties().get(Constants.NAMESPACE_PREFIX_PROPERTY_NAME);
		String inputParmsXPath = exeInfo.getOtherProperties().get(DocumentBuilder.PARM_INPUT_XPATH_TABLE_PROPERTY_NAME); 
		SOAPDocBuilder builder = new SOAPDocBuilder();
		doc = builder.createXMLRequestDoc(bodyParams, (SOAPConnectorState)state, namespacePrefixes, inputParmsXPath);
	}

	public int getDocumentCount() throws ConnectorException {
		return 1;
	}
	
    private void sortParams(List<CriteriaDesc> allParams, List<CriteriaDesc> headerParams, List<CriteriaDesc> bodyParams) throws ConnectorException {
		// sort the parameter list into header and body content
		// replace this later with model extensions
		for (CriteriaDesc desc : allParams)
			if (desc.getInputXpath().startsWith(SOAPDocBuilder.soapNSLabel+ ":" + SOAPDocBuilder.soapHeader)) { //$NON-NLS-1$
				headerParams.add(desc);
			} else {
				bodyParams.add(desc);
			}
	}

}
