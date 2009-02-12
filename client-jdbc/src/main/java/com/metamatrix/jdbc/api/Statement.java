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

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.util.Collection;


/**
 * The MetaMatrix-specific interface for executing statements against 
 * the MetaMatrix server.  This interface provides methods in 
 * addition to the standard JDBC methods. 
 */
public interface Statement extends java.sql.Statement {

    /**
     * Get the MetaMatrix-specific execution property value.
     * @param name Execution property name
     * @return Execution property value or null if not set
     */
    String getExecutionProperty(String name);

    /**
     * Set the MetaMatrix-specific execution property value.
     * @param name Execution property name
     * @param value Execution property value 
     */
    void setExecutionProperty(String name, String value);

    /**
     * Obtain the query plan object representation from the last 
     * command executed on this Statement, if a query plan was 
     * requested in the command.  If no plan was requested, this 
     * method will return null.
     * @return PlanNode representing the root of the query plan
     */
    PlanNode getPlanDescription();
    
    /**
     * Obtain the query planner debug log from the last command 
     * executed on this Statement, if it was requested with
     * OPTION DEBUG.  If no debug output was requested, this 
     * method will return null. 
     * @return Debug log or null if no log exists
     */
    String getDebugLog();
    
    /**
     * Get collection of annotations from the query planner from
     * the last command executed on the Statement, if annotations
     * were requested.  If no annotation was requested, this method
     * will return null  
     * @return Collection of {@link Annotation}s
     */
    Collection getAnnotations();
     
    /**
     * Attach a stylesheet to be applied on the server for XML queries 
     * executed with this Statement.
     * @param reader Reader for reading a stylesheet in XML
     * @throws IOException If an error occurs reading the stylesheet
     */
    void attachStylesheet(Reader reader) throws IOException;    

    /**
     * Clear any previously attached stylesheet for this Statement object.
     */
    void clearStylesheet();    

    /**
     * Get ID for last execution which can be used for matching up executions
     * on the client side with executions in the server logs and MetaMatrix console.
     * @return String identifier for the last execution
     */
    String getRequestIdentifier();

    
    /**
     * Set the per-statement security payload.  This optional payload will 
     * accompany each request to the data source(s) so that the connector
     * will have access to it.
     * <br>Once the payload is set, it will be used for each statment
     * execution until it is set to <code>null</code>, a new payload is set on
     * the statement or the statement is closed.</br>
     * 
     * <p>To remove an existing payload from a statement, call this method
     * with a <code>null</code> argument.</p>
     *   
     * <p>The execution payload differs from the Trusted Payload in that it
     * is set on the Statement and so may not be constant over the Connection lifecycle
     * and may be changed upon each statement execution.  The Execution Payload is
     * <em>not</em> authenticated or validated by the MetaMatrix system.</p>
     * 
     * <p>Given that the Execution Payload is not authenticated by the MetaMatrix
     * system, connector writers are responsible for ensuring its validity.  This
     * can possibly be accomplished by comparing it against the Trusted Payload.</p>
     * 
     * @param payload The payload that is to accompany requests executed
     * from this statement.
     * @since 4.2
     */
    void setPayload(Serializable payload);
}
