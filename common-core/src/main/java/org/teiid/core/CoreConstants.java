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
