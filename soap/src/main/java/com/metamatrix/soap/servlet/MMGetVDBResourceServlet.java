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

package com.metamatrix.soap.servlet;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.util.WSDLServletUtil;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.log.FileLogWriter;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.soap.SOAPPlugin;
import com.metamatrix.soap.util.ErrorMessageKeys;
import com.metamatrix.soap.util.SOAPConstants;
import com.metamatrix.soap.util.WebServiceUtil;
 

/** 
 * Servlet to retrieve WSDL from a VDB
 * @since 4.2
 */
public class MMGetVDBResourceServlet extends HttpServlet {
    

    
    protected static final String procString = "{?=call System.getUpdatedCharacterVDBResource(?,?,?)}"; //$NON-NLS-1$
    protected static final String DATASERVICE = "/services/service"; //$NON-NLS-1$
    protected static final String[] TOKEN_ARRAY = {CoreConstants.URL_ROOT_FOR_VDB,
                                                 CoreConstants.URL_SUFFIX_FOR_VDB,
                                                 CoreConstants.URL_FOR_DATA_WEBSERVICE,
                                                 CoreConstants.ACTION_PREFIX_FOR_DATA_WEBSERVICE};
    protected static final String WSDL_ERROR = "wsdlerror"; //$NON-NLS-1$
    
    protected static final String AMP = "&"; //$NON-NLS-1$
    
    protected static final String EQUALS = "="; //$NON-NLS-1$ 
        
    /** DataService Endpoint */
    private String dataServiceEndpoint = StringUtil.Constants.EMPTY_STRING;
    
    static Logger log = Logger.getLogger(MMGetVDBResourceServlet.class);
    
    public MMGetVDBResourceServlet() {
    }
    
    synchronized public void init(ServletConfig config) throws ServletException {
        super.init(config);
        
        /* Check for override of Data Service endpoint. This is to allow for backwards compatibility
         * of WSDL for pre-5.5 data services.
         */
        dataServiceEndpoint=getServletContext().getInitParameter("endpointOverride"); //$NON-NLS-1$
        if (dataServiceEndpoint==null || dataServiceEndpoint.equals(StringUtil.Constants.EMPTY_STRING)){
        	dataServiceEndpoint=DATASERVICE;
        }
    }
    
    
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
        doPost(req, resp);
    }

    /** 
     * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     * @since 4.2
     */
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
                
    	Connection connection = null;
    	
        //Set the default content type. If we get a resource, we will change this to text/xml.
        resp.setContentType(WSDLServletUtil.DEFAULT_CONTENT_TYPE);
        
        //set error in header, clear after successful wsdl return
        resp.setHeader(WSDL_ERROR, WSDL_ERROR);
        
        // pull out the form variables
        String serverURL = 
            req.getParameter(WSDLServletUtil.SERVER_URL_KEY);        
        String vdbName = 
            req.getParameter(WSDLServletUtil.VDB_NAME_KEY);
        String vdbVersion =
            req.getParameter(WSDLServletUtil.VDB_VERSION_KEY);
        String resourcePath = req.getPathInfo(); 
        
        //Validate parameters
        try {
            checkFormValue(serverURL, WSDLServletUtil.SERVER_URL_KEY);
            checkFormValue(vdbName, WSDLServletUtil.VDB_NAME_KEY);                        
            
            serverURL += (";" + SOAPConstants.APP_NAME + "=" + //$NON-NLS-1$ //$NON-NLS-2$
            SOAPPlugin.Util.getString("MMGetVDBResourceServlet.Application_Name")); //$NON-NLS-1$
            
        } catch (Exception e) {
            log.error(e);
            resp.getOutputStream().println(e.getMessage());
            return;
        }
                
        try {
            connection = getConnection(WebServiceUtil.WSDLUSER, WebServiceUtil.WSDLPASSWORD, vdbName, vdbVersion, serverURL);
        } catch (Exception e) {
            String message = SOAPPlugin.Util.getString(ErrorMessageKeys.SERVICE_0006); //;
            log.error(message, e);
            resp.getOutputStream().println(message);
            return;
        }
        
        String suffix = buildEndpointSuffixString(req.getParameterMap()); 
        
        String urlPrefix = buildUrlPrefix(req);
                	
        String servletPath = urlPrefix+WSDLServletUtil.SERVLET_PATH; //$NON-NLS-1$ 
                
        String result = escapeAttributeEntities(suffix);
        
        result="?"+result; //$NON-NLS-1$
        
        String endPointURL = null;
           
        endPointURL = urlPrefix+dataServiceEndpoint+result; 

        /* Need to create a string of server properties to set as the prefix for the
         * action value. The suffix will be the procedure name in the form 
         * of "procedure=fully.qualified.procedure.name".
         */        
        StringBuffer serverProperties = new StringBuffer();
        
        serverProperties.append(WSDLServletUtil.VDB_NAME_KEY).append(EQUALS).append(vdbName);
        serverProperties.append(AMP).append(WSDLServletUtil.SERVER_URL_KEY).append(EQUALS).append(serverURL.substring(0, serverURL.indexOf(";"))); //$NON-NLS-1$
        serverProperties.append(AMP).append(WSDLServletUtil.VDB_VERSION_KEY).append(EQUALS).append(vdbVersion!=null?vdbVersion:StringUtil.Constants.EMPTY_STRING);
        serverProperties.append(AMP);
        
        serverProperties = new StringBuffer(escapeAttributeEntities(serverProperties.toString())); 
        
        try {
            getResource(resp, 
                        procString, 
                        resourcePath,
                        servletPath,                        
                        result,
                        serverProperties.toString(),
                        endPointURL,
                        connection);

        } catch (SQLException se) {            
            resp.getOutputStream().println(se.getMessage());
            log.error(SOAPPlugin.Util.getString("MMGetVDBResourceServlet.7"), se);
        } catch (Exception e) {
            log.error(SOAPPlugin.Util.getString("MMGetVDBResourceServlet.8"), e);
            resp.getOutputStream().println(e.getMessage());        
        } finally {
            try {
                //Cleanup our connection
                connection.close();
            } catch (SQLException e) {
                log.error(SOAPPlugin.Util.getString("MMGetVDBResourceServlet.0"), e);
                resp.setHeader(WSDL_ERROR, WSDL_ERROR);
                resp.getOutputStream().println(e.getMessage());
            }
        }
        
    }

	/**
	 * @param req
	 * @param httpTypeString
	 * @return urlPrefix
	 */
	private String buildUrlPrefix(HttpServletRequest req) {
		StringBuffer urlPrefix = new StringBuffer();
		if (req.isSecure()){
        	urlPrefix.append(WSDLServletUtil.HTTPS).append("://").append(req.getServerName()).append(":").append(WSDLServletUtil.getHttpsPort()); //$NON-NLS-1$ //$NON-NLS-2$
        }else{
        	urlPrefix.append(WSDLServletUtil.HTTP).append("://").append(req.getServerName()).append(":").append(WSDLServletUtil.getHttpPort()); //$NON-NLS-1$ //$NON-NLS-2$        	
        }
		urlPrefix.append(req.getContextPath());
        return urlPrefix.toString();        
	}
             
    /**
     * Get a JDBC connection. Will create a connection. It takes Userid, Password, 
     * URL, VDBName/version
     *   
     */
    public Connection getConnection(String userid,
                               String password,
                               String vdbName,
                               String vdbVersion,
                               String url) throws SQLException, ClassNotFoundException {
    	
    	    //Create Properties object and add username\password for connection
    		Properties props = new Properties();
    		props.put(MMURL.CONNECTION.USER_NAME, userid);
    		props.put(MMURL.CONNECTION.PASSWORD, password);
    		
            Class.forName("com.metamatrix.jdbc.MMDriver");  //$NON-NLS-1$
                            
            return DriverManager.getConnection("jdbc:metamatrix:" //$NON-NLS-1$ 
                                                     + vdbName 
                                                     + "@" //$NON-NLS-1$
                                                     + url 
                                                     + (vdbVersion==null || vdbVersion.trim().length()==0 ? StringUtil.Constants.EMPTY_STRING : ";version="+vdbVersion), props); //$NON-NLS-1$
            
    }        
    
    /**
     * Execute a call against the VDB to retrieve it's WSDL. 
     *       
     */
    public void getResource(HttpServletResponse resp, 
                             String procString, 
                             String resourcePath,
                             String urlPrefix,
                             String urlSuffix,
                             String serverProperites,
                             String endPointURL,
                             Connection connection) throws SQLException, IOException {

    	/** Value array to pass into resource procedure*/
        String[] value_Array = new String[4]; 
        
        value_Array[0]=urlPrefix;
        value_Array[1]=urlSuffix;
        value_Array[2]=endPointURL;
        value_Array[3]=serverProperites;
                
        CallableStatement statement = connection.prepareCall(procString);
        ResultSet rs = null;
        Clob clob = null;
        
        statement.setString(1, resourcePath);
        statement.setObject(2, TOKEN_ARRAY);
        statement.setObject(3, value_Array);
               
        boolean rtn = statement.execute();
        
        if (rtn==true) {
            rs = statement.getResultSet();
        }else {
            String message = SOAPPlugin.Util.getString("MMGetVDBResourceServlet.12"); //$NON-NLS-1$
            log.error(message);
            resp.getOutputStream().println(message); 
            return;
        }
        
        if (rs.next()) {
            clob = rs.getClob(1);                      
        }else {
            String message = SOAPPlugin.Util.getString("MMGetVDBResourceServlet.14"); //$NON-NLS-1$
            log.error(message);
            resp.getOutputStream().println(message); 
            return;
        }
                    
        //Stream Clob to response
        Reader reader = clob.getCharacterStream();
        
        StringWriter sw = new StringWriter();
        
        while(true) {
            int c = reader.read();
            if(c == -1) {
                break;
            }
            sw.write(c);           
        }
        
        //since successful, clear the error field in the header
        resp.setHeader(WSDL_ERROR, null);
        
        //we know we have XML, so change the content type accordingly 
        resp.setContentType(WSDLServletUtil.XML_CONTENT_TYPE); 
        
        //write out the WSDL
        resp.getOutputStream().write(sw.getBuffer().toString().getBytes());
    }
    
    /**
     * Internal helper method to verify that a form value has data behind it
     * @throws Exception if this is not the case
     */
    private static void checkFormValue(String parameter, String expectedParameterName)
        throws Exception {
        if (parameter == null || parameter.trim().length() == 0) {
            throw new Exception(SOAPPlugin.Util.getString(ErrorMessageKeys.SERVICE_0004, expectedParameterName));
        }
    }
    
    /**
     * <p>
     * This will take the pre-defined entities in XML 1.0 and
     *   convert their character representation to the appropriate
     *   entity reference, suitable for XML attributes.  It does
     *   no conversion for ' because it's not necessary as the outputter 
     *   writes attributes surrounded by double-quotes.
     * </p>
     *
     * @param st <code>String</code> input to escape.
     * @return <code>String</code> with escaped content.
     */
    protected String escapeAttributeEntities(String st) {
        StringBuffer buff = new StringBuffer();
        char[] block = st.toCharArray();
        String stEntity = null;
        int i, last;

        for (i=0, last=0; i < block.length; i++) {
            switch(block[i]) {
                case '<' :
                    stEntity = "&lt;"; //$NON-NLS-1$
                    break;
                case '>' :
                    stEntity = "&gt;"; //$NON-NLS-1$
                    break;
                case '\"' :
                    stEntity = "&quot;"; //$NON-NLS-1$
                    break;
                case '&' :
                    stEntity = "&amp;"; //$NON-NLS-1$
                    break;
                default :
                    /* no-op */ 
            }
            if (stEntity != null) {
                buff.append(block, last, i - last);
                buff.append(stEntity);
                stEntity = null;
                last = i + 1;
            }
        }
        if(last < block.length) {
            buff.append(block, last, i - last);
        }

        return buff.toString();
    }

    /**
     * This method is used to filter out the unnecessary httpType request parameter that is used to 
     * drive the protocol that should be used on the endpoint URL.  This httptype parameter is not
     * necessary on the endpoint URL. 
     * 
     * @param parameterMap The input Map of parameters
     * @return a 'query string' that represents the remainder of the parameters.
     * @since 4.3
     */
    protected String buildEndpointSuffixString(Map parameterMap) {
        String suffixString = StringUtil.Constants.EMPTY_STRING;
        Set keySet = parameterMap.keySet();
        Iterator iter = keySet.iterator();
        while (iter.hasNext()) {
            Object key = iter.next();
            if (key instanceof String) {
                String keyString = (String)key;
                if (!StringUtil.Constants.EMPTY_STRING.equals(suffixString)) {
                        suffixString = suffixString + "&"; //$NON-NLS-1$
                }
                String parameterValue = StringUtil.Constants.EMPTY_STRING;
                String[] paramValueArray = (String[])parameterMap.get(keyString);
                if (paramValueArray.length > 0) {
                    try {
                        // make sure to encode the parameter values so they do not violate URL protocols.
                        parameterValue = URLEncoder.encode(((String[])parameterMap.get(keyString))[0], "UTF-8"); //$NON-NLS-1$
                    } catch (UnsupportedEncodingException err) {
                        log.error(SOAPPlugin.Util.getString("MMGetVDBResourceServlet.15", err));
                    }
                }
                suffixString = suffixString + keyString + "=" + parameterValue; //$NON-NLS-1$ 
            }
        }
        return suffixString;
        
    }
 
    /**
	 * Get scheme value (http/https) for building the url
	 * 
	 * @param isSecure - boolean
	 * @return vdbVersion
	 * @since 5.5.3
	 */
	public static String getScheme(boolean isSecure) {
		
		if (isSecure){
			return WSDLServletUtil.HTTPS;
		}
		return WSDLServletUtil.HTTP;
	}
}   

