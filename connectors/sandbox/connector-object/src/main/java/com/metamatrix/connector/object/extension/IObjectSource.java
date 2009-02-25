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

package com.metamatrix.connector.object.extension;

import java.util.List;

import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.object.ObjectProcedureExecution;

/**
 * <p>
 * This interface represents the connection to the source api.
 * The IObjectSource is wrapped within the {@link ObjectConnection} and given to the
 * {@link Execution} object (currently, only the {@link ObjectProcedureExecution})
 * for executiong a command and returning objects in a List.   The returned object List
 * does not have to be in a converted form.  That conversion, based on the model, will 
 * be done when the batches are processed for return {@link ObjectProcedureExecution}. 
 * </p>
 * <p>
 * There is a base implementation {@link BaseObjectSource} that provides the implementation
 * for the {@see #getObjects(IObjectCommand)}.
 * </p>  

 */
public interface IObjectSource  {
    

    /**
     * Retrieve objects satisfying a {@link IObjectCommand command}.
     * 
     * This yields a List of results that the ObjectResultsTranslator will translate for the returned resultset.
     * 
     * @param command The command that defines the group and criteria to use to get the objects
     * @return Iterator on Lists of result objects.  Each List is a batch of results.
     */
    List getObjects(IObjectCommand command) throws ConnectorException;
   
    /**
     * Return the object that is the source.  It is the object that will
     * be used to execute the methods on.
     */
    Object getSource();
    
    /**
     * Determine whether the source is open
     * @return True if open, false if closed or failed.
     */    
    boolean isAlive();

    /**
     * Determine whether the source has failed.
     * @return True if failed, false if open or closed without failure.
     */    
    boolean isFailed();
    
    
    /**
     * Close the underlying source connection
     * @throws ConnectorException If an error occured while closing
     */
    void closeSource() throws ConnectorException;

}
