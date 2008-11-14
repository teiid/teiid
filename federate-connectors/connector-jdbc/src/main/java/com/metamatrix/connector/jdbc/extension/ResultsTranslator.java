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

/*
 */
package com.metamatrix.connector.jdbc.extension;

import java.sql.*;

import java.util.List;
import java.util.TimeZone;

import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ICommand;

/**
 * Specify database-specific behavior for translating results.
 */
public interface ResultsTranslator {
    
    /**
     * Initialize the results translator with the connector's environment, which
     * can be used to retrieve configuration parameters
     * @param env The connector environment
     * @throws ConnectorException If an error occurs during initialization
     */
    void initialize(ConnectorEnvironment env) throws ConnectorException; 

    /**
     * Get a list of ValueTranslator objects that specify database-specific value
     * translation logic.  By default, the JDBC connector has a large set of available
     * translator.
     * @return List of ValueTranslator
     */
    List getValueTranslators();

    /**
     * Used to specify a special database-specific value retriever.  By default, the BasicValueRetriever
     * will be used to retrieve objects from the ResultSet via the getObject() method. 
     * @return The specialized ValueRetriever
     */
    ValueRetriever getValueRetriever();     
    
    /**
     * Execute a stored procedure - this is often database-dependent.
     * @param stmt The CallableStatement created by the connector
     * @param command The translated command information
     * @return The ResultSet returned by the stored procedure
     * @throws SQLException If an error occurs during execution
     */
    ResultSet executeStoredProcedure(CallableStatement stmt, TranslatedCommand command) throws SQLException ;
    
    /**
     * Populate the prepared statement.
     * The prepared statement values will be set.
     * @param conn
     * @param stmt
     * @param command
     * @throws SQLException
     */
    public void bindPreparedStatementValues(Connection conn, PreparedStatement stmt, TranslatedCommand command) throws SQLException;
        
    /**
     * Execute the bulk insert statement. 
     * @param conn The connection
     * @param stmt The prepared statement with the query
     * @param command The translated command, which contains the large objects to prepare with
     * @return Update count for the execution
     * @throws SQLException If an error occurs during execution
     */
    public int executeStatementForBulkInsert(Connection conn, PreparedStatement stmt, TranslatedCommand command) throws SQLException;    
        
    /**
     * Determine the time zone the database is located in.  Typically, this time zone is 
     * the same as the local time zone, in which case, null should be returned.
     * @return Database time zone
     */
    TimeZone getDatabaseTimezone();
    
    /**
     * Allow an extension to modify any batch before it is returned to MetaMatrix.  This
     * method can be used to apply row-level entitlements, modify data values for particular
     * users, etc. 
     * @param batch The batch to return
     * @param context The execution context for the query 
     * @param command The modified command that was obtained from the translation visitor provided by the SQLTranslator
     * @return The batch to use instead
     * @since 4.2
     */
    Batch modifyBatch(Batch batch, ExecutionContext context, ICommand command);    
    
    /**
     * Get the max result rows allowed
     * 
     * @return the max result rows
     */
    int getMaxResultRows();
    
    
    TypeFacility getTypefacility();
}
