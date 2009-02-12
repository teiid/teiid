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

package com.metamatrix.jdbc.api;

/**
 * MetaMatrix-specific execution properties.  These execution properties can 
 * be set via the {@link com.metamatrix.jdbc.api.Statement#setExecutionProperty(String, String)}
 * method.  They affect the subsequent execution of all commands on that Statement
 * instance.   
 */
public interface ExecutionProperties {

    
    
    /** Execution property name for XML format */
    public static final String PROP_XML_FORMAT = "XMLFormat"; //$NON-NLS-1$
    
    /** Execution property name for XML validation */
    public static final String PROP_XML_VALIDATION = "XMLValidation"; //$NON-NLS-1$

    /** Execution property name for transaction auto wrap mode */
    public static final String PROP_TXN_AUTO_WRAP = "txnAutoWrap"; //$NON-NLS-1$

    /** Execution property name for partial results mode */
    public static final String PROP_PARTIAL_RESULTS_MODE = "partialResultsMode"; //$NON-NLS-1$

    /** XML results format:  XML results displayed as a formatted tree */
    public static final String XML_TREE_FORMAT = "Tree"; //$NON-NLS-1$

    /** XML results format:  XML results displayed in compact form */
    public static final String XML_COMPACT_FORMAT = "Compact"; //$NON-NLS-1$

    /** Transaction auto wrap constant - never wrap a command execution in a transaction
     *  and allow multi-source updates to occur outside of a transaction. */
    public static final String AUTO_WRAP_OFF = "OFF"; //$NON-NLS-1$

    /** Transaction auto wrap constant - always wrap every non-transactional command
     *  execution in a transaction. */
    public static final String AUTO_WRAP_ON = "ON"; //$NON-NLS-1$

    /** Transaction auto wrap constant - pessimistic mode assumes that any command
     *  execution might require a transaction to be wrapped around it.  To determine
     *  this an extra server call is made to check whether the command requires
     *  a transaction and a transaction will be automatically started.  This is most
     *  accurate and safe, but has a performance impact. */
    public static final String AUTO_WRAP_PESSIMISTIC = "PESSIMISTIC"; //$NON-NLS-1$

    /** Transaction auto wrap constant */
    public static final String AUTO_WRAP_OPTIMISTIC = "OPTIMISTIC"; //$NON-NLS-1$

    /** 
     * Whether to use result set cache if it is available 
     * @since 4.2 
     */
    public static final String RESULT_SET_CACHE_MODE = "resultSetCacheMode"; //$NON-NLS-1$
    
    /**
     * Default fetch size to use on Statements if the fetch size is not explicitly set.
     * The default is 500.  
     * @since 4.2
     */
    public static final String PROP_FETCH_SIZE = "fetchSize";   //$NON-NLS-1$ 
    
    /**
     * If true, will ignore autocommit for local transactions.
     * @since 5.5.2
     */
    public static final String DISABLE_LOCAL_TRANSACTIONS = "disableLocalTxn";  //$NON-NLS-1$
    
    /**
	 * By default treat the double quoted strings as variables in a
	 * ODBC connection. This is to allow the metadata tools based on
	 * ODBC to work seemlessly.
	 * @since 4.3 
     */
    public static final String ALLOW_DBL_QUOTED_VARIABLE = "allowDoubleQuotedVariable"; //$NON-NLS-1$   
    
    /**
     * Additional options/hints for executing the command
     * @since 4.3
     */
    public static final String PROP_SQL_OPTIONS = "sqlOptions"; //$NON-NLS-1$
    
    /**
     * Passed as an option to PROP_SQL_OPTIONS
     */
    public static final String SQL_OPTION_SHOWPLAN = "SHOWPLAN"; //$NON-NLS-1$
    
    public static final String PLAN_NOT_ALLOWED = "planNotAllowed"; //$NON-NLS-1$
}
    