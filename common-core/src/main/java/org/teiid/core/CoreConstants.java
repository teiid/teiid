/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.core;


/**
 * @since 4.0
 */
public interface CoreConstants {
    /**<p>
     * Contains ID's for all MetaMatrix plugins.
     * </p>
     * @since 4.0
     */
    interface Plugin {
        String METAMATRIX_PREFIX = "com.metamatrix."; //$NON-NLS-1$
        
        String COMMON               = METAMATRIX_PREFIX + "common"; //$NON-NLS-1$
        String COMMON_COMM_PLATFORM = METAMATRIX_PREFIX + "common.comm.platform"; //$NON-NLS-1$
        String CORE_XSLT            = METAMATRIX_PREFIX + "core.xslt"; //$NON-NLS-1$
        String MODELER_JDBC         = METAMATRIX_PREFIX + "modeler.jdbc"; //$NON-NLS-1$
        String PLATFORM             = METAMATRIX_PREFIX + "platform"; //$NON-NLS-1$
    }
    
    interface Debug {
        String PLUGIN_ACTIVATION = "pluginActivation"; //$NON-NLS-1$
    }

    interface Trace {
        String PROXIES = "proxies"; //$NON-NLS-1$
    }

    public static final String SYSTEM_MODEL = "SYS"; //$NON-NLS-1$
    
    public static final String SYSTEM_ADMIN_MODEL = "SYSADMIN"; //$NON-NLS-1$
    
    public static final String ODBC_MODEL = "pg_catalog"; //$NON-NLS-1$
    
    public static final String SYSTEM_VDB = "System.vdb"; //$NON-NLS-1$

    public static final String PRODUCT_OWNER_NAME_PROPERTY = "ownerName"; //$NON-NLS-1$
    
    /**
     * Constant that defines the replaceable token in WSDL generated in the VDB which represents
     * the MetaMatrix Server properties for connecting the to VDB.
     */
    public static final String ACTION_PREFIX_FOR_DATA_WEBSERVICE = "http://server.properies.metamatrix.com"; //$NON-NLS-1$
    
    /**
     * Constant that defines the parameter value in WSDL generated of the VDB that will be used to 
     * identifiy the fully qualified procedure name when executing the web service.
     */
    public static final String ACTION_PARAMETER_FOR_DATA_WEBSERVICE_PROCEDURE = "procedure="; //$NON-NLS-1$
    
    /**
     * Constant that defines the replaceable token in WSDL generated in the VDB which represents
     * the first part (VDB-independent) of the URL.  The general form of the URLs is as follows:
     * <p>
     * <code>  [URL ROOT][Path to Resource in VDB][URL Suffix]</code>
     * </p></p>
     * In general, the URL root would be replaced in a particular server with the actual URL to the servlet 
     * used to obtain the WSDL and XSD, and the URL suffix replaced with the servlet parameters.
     * </p>
     * @see #URL_SUFFIX_FOR_VDB
     */
    public static final String URL_ROOT_FOR_VDB = "http://vdb.metamatrix.com"; //$NON-NLS-1$
    
    /**
     * Constant that defines the replaceable token in WSDL generated in the VDB which represents
     * the suffix part of the URL.  The general form of the URLs is as follows:
     * <p>
     * <code>  [URL ROOT][Path to Resource in VDB][URL Suffix]</code>
     * </p></p>
     * In general, the URL root would be replaced in a particular server with the actual URL to the servlet 
     * used to obtain the WSDL and XSD, and the URL suffix replaced with the servlet parameters.
     * </p>
     * @see #URL_ROOT_FOR_VDB
     */
    public static final String URL_SUFFIX_FOR_VDB = "?vdbToken=true"; //$NON-NLS-1$
    
    
    /**
     * Constant that defines the replaceable token in WSDL generated in the VDB which represents
     * the URL for the service binding.
     * @see #URL_SUFFIX_FOR_VDB
     * @see #URL_ROOT_FOR_VDB
     */
    public static final String URL_FOR_DATA_WEBSERVICE = "http://vdb.dataservice.metamatrix.com"; //$NON-NLS-1$\
    
    /**
     * Constant for the anonymous Teiid system username  
     */
    public static final String DEFAULT_ANON_USERNAME = "anonymous"; //$NON-NLS-1$
    

}
