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

package org.teiid.query.processor.xml;

import java.util.List;
import java.util.Map;

import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


/**
 * This interface is responsible for executing the queries for the XML processing
 * environment.
 */
public interface PlanExecutor {

    /**
     * Execute the plan   
     * @param referenceValues - values for any external references
     * @param openOnly
     */
    public void execute(Map referenceValues, boolean openOnly) throws TeiidComponentException, BlockedException, TeiidProcessingException;

    /**
     * Get the ElementSymbol list which represents the schema of the result set
     * @return
     * @throws TeiidComponentException
     */
    public List getOutputElements() throws TeiidComponentException;
    
    /**
     * Advance the resultset cursor to next row and retun the row values
     * @return values of the row
     */
    public List nextRow() throws TeiidComponentException, TeiidProcessingException;

    /**
     * Get the result set values of the current row.
     * @return
     */
    public List currentRow() throws TeiidComponentException, TeiidProcessingException;

    /**
     * close the plan and cleanup the resultset.
     */
    public void close() throws TeiidComponentException;
}
