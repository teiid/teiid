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

package com.metamatrix.connector.sysadmin.extension;

import java.util.List;

import com.metamatrix.data.exception.ConnectorException;

/**
 * <p>
 * This interface represents the connection to the system admin.  The implementation is 
 * responsible for making the connection to the system administration api's.
 * </p>

 */
public interface ISysAdminSource  {
    

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
