/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.soap.util;

import com.metamatrix.soap.util.SOAPConstants;

/**
 * This file is used for constants to be displayed on the SOAP Client .jsp page.
 * It is used by the com.metamatrix.soap.service.MMSOAPClientServlet to pull
 * the form values from the .jsp page in order to submit a soap service query
 */

public final class ServletClientConstants {

	private ServletClientConstants(){
	}
	/**
	 * General keys
	 */
	public static final String LOGON_KEY	 	    = "Logon"; //$NON-NLS-1$
	public static final String PASSWORD_KEY 		= "Password"; //$NON-NLS-1$
	public static final String SESSIONID_KEY		= "SessionID"; //$NON-NLS-1$
	public static final String SERVER_URL_KEY		= "ServerURL"; //$NON-NLS-1$
	public static final String SOAP_URL_KEY			= "SOAPURL"; //$NON-NLS-1$
	public static final String SOAP_SERVICE_URN_KEY	= "SOAPServiceURN"; //$NON-NLS-1$
	public static final String VDB_NAME_KEY			= "VDBName"; //$NON-NLS-1$
	public static final String VDB_VERSION_KEY		= "VDBVersion"; //$NON-NLS-1$
	public static final String FETCH_SIZE_KEY		= "FetchSize"; //$NON-NLS-1$
	public static final String FULL_METADATA_KEY	= "FullMetadata"; //$NON-NLS-1$
	public static final String PARTIAL_RESULTS_KEY	= "PartialResults"; //$NON-NLS-1$
	public static final String TIMEOUT_KEY			= "TimeOut"; //$NON-NLS-1$

	public static final String DEBUG_KEY			= "Debug"; //$NON-NLS-1$

	/**
	 * The execute parameters
	 */
	public static final String EXECUTE_QUERY_KEY			= "executeQuery"; //$NON-NLS-1$
	public static final String EXECUTE_UPDATE_KEY			= "executeUpdate"; //$NON-NLS-1$
	public static final String EXECUTE_STORED_PROCEDURE_KEY	= "executeStoredProcedure"; //$NON-NLS-1$

	public static final String EXECUTE_METADATA_KEY			= "executeMetadata"; //$NON-NLS-1$
	public static final String EXECUTE_LOGON_KEY			= "executeLogon"; //$NON-NLS-1$
	public static final String EXECUTE_LOGOFF_KEY			= "executeLogoff"; //$NON-NLS-1$
    public static final String EXECUTE_STORED_QUERY         = "executeStoredQuery"; //$NON-NLS-1$
    /*
     * For the stored queries to query through the MetaMatrixServerAPI vs the
     * DataService web service
     */
     public static final String EXECUTE_STORED_QUERY_API_KEY = "executeStoredQueryAPI"; //$NON-NLS-1$

	/**
	 * For the metadata
	 */
	public static final String METADATA_RADIO_BUTTON_KEY	="rdo_metadata"; //$NON-NLS-1$
	public static final String METADATA_PREPEND_KEY 		= "METADATA_"; //$NON-NLS-1$
	public static final String CR_PRIMARY_GROUP_NAME_KEY 	= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.CROSS_REFERENCES.Parameters.PRIMARY_GROUP_NAME;
	public static final String CR_FOREIGN_GROUP_NAME_KEY 	= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.CROSS_REFERENCES.Parameters.FOREIGN_GROUP_NAME;
	public static final String ELEMENTS_GROUP_NAME_KEY	= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.ELEMENTS.Parameters.GROUP_PATTERN;
	public static final String ELEMENTS_ELEMENT_NAME_KEY 	= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.ELEMENTS.Parameters.ELEMENT_PATTERN;
	public static final String EK_PRIMARY_GROUP_NAME_KEY	= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.EXPORTED_KEYS.Parameters.PRIMARY_GROUP_NAME;
	public static final String FK_PRIMARY_GROUP_NAME_KEY 	= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.FOREIGN_KEYS.Parameters.GROUP_NAME;
	public static final String IK_FOREIGN_GROUP_NAME_KEY	= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.IMPORTED_KEYS.Parameters.FOREIGN_GROUP_NAME;
	public static final String GROUP_NAME_KEY				= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.GROUPS.Parameters.GROUP_PATTERN;
	public static final String PK_GROUP_NAME_KEY			= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.PRIMARY_KEYS.Parameters.GROUP_NAME;
	public static final String GP_PROCEDURE_NAME_KEY		= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.PROCEDURE_PARAMETERS.Parameters.PROCEDURE_NAME_PATTERN;
	public static final String GP_PARAMETER_NAME_KEY		= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.PROCEDURE_PARAMETERS.Parameters.PARAMETER_NAME_PATTERN;
	public static final String P_PROCEDURE_NAME_KEY		= METADATA_PREPEND_KEY+SOAPConstants.METADATA_TYPES.PROCEDURES.Parameters.PROCEDURE_PATTERN;

	/**
	 * For the XML
	 *
	 */
	public static final String CB_RETURN_SCHEMA 	= "cb_returnSchema"; //$NON-NLS-1$
	public static final String CB_VALIDATE_DOCUMENT = "cb_validateDocument"; //$NON-NLS-1$

	public static final String COMPACT_FORMAT_KEY	= "xml_compact_format"; //$NON-NLS-1$
	/**
	 * For standard quering
	 */
	public static final String SQL_KEY				= "SQL"; //$NON-NLS-1$

	/**
	 * For the Stored Procedures
	 */
	public static final String SP_PREPEND					= "sp_"; //$NON-NLS-1$
	public static final String SEP_STRING					= "."; //$NON-NLS-1$
	public static final String SP_PROCEDURE_PARAM_VALUE_KEY = SP_PREPEND+"sp_procedure_param_value_key"; //$NON-NLS-1$
	public static final String SP_PROCEDURE_NAME_KEY 		= SP_PREPEND+"sp_procedure_name_key"; //$NON-NLS-1$
    
    /**
     * For the Stored Queries
     */
    public static final String SQ_PREPEND         = "sq_"; //$NON-NLS-1$
    public static final String SQ_SEP_STRING      = "."; //$NON-NLS-1$
    public static final String SQ_PARAM_VALUE_KEY = "sq_param_value_key"; //$NON-NLS-1$
    public static final String SQ_NAME_KEY        = "sq_name_key"; //$NON-NLS-1$
    

    /**
     * For the Stored Queries through the MetamatrixServerAPI Web service
     */
    public static final String SQ_PREPEND_API         = "sq_api"; //$NON-NLS-1$
    public static final String SQ_SEP_STRING_API      = "."; //$NON-NLS-1$
    public static final String SQ_PARAM_VALUE_KEY_API = "sq_api_param_value_key"; //$NON-NLS-1$
    public static final String SQ_NAME_KEY_API        = "sq_api_name_key"; //$NON-NLS-1$
}

