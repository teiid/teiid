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

package org.teiid.jdbc;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.util.Collection;

import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.PlanNode;



/**
 * This interface provides methods in 
 * addition to the standard JDBC methods. 
 */
public interface TeiidStatement extends java.sql.Statement {

    /**
     * Get the execution property value.
     * @param name Execution property name
     * @return Execution property value or null if not set
     */
    String getExecutionProperty(String name);

    /**
     * Set the execution property value.
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
     * the last command executed on the Statement  
     * @return Collection of {@link Annotation}s, may return null
     */
    Collection<Annotation> getAnnotations();
     
    /**
     * Attach a stylesheet to be applied on the server for XML queries 
     * executed with this Statement.
     * @param reader Reader for reading a stylesheet in XML
     * @throws IOException If an error occurs reading the stylesheet
     * @deprecated
     */
    void attachStylesheet(Reader reader) throws IOException;    

    /**
     * Clear any previously attached stylesheet for this Statement object.
     * @deprecated
     */
    void clearStylesheet();    

    /**
     * Get ID for last execution which can be used for matching up executions
     * on the client side with executions in the server logs.
     * @return String identifier for the last execution
     */
    String getRequestIdentifier();
    
    /**
     * Set the per-statement security payload.  This optional payload will 
     * accompany each request to the data source(s) so that the connector
     * will have access to it.
     * 
     * <p>To remove an existing payload from a statement, call this method
     * with a <code>null</code> argument.</p>
     *   
     * @param payload The payload that is to accompany requests executed
     * from this statement.
     * @since 4.2
     */
    void setPayload(Serializable payload);
}
