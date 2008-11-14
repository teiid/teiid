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

package com.metamatrix.common.pooling.impl;

import java.sql.SQLException;

import com.metamatrix.common.pooling.api.Resource;
import com.metamatrix.common.pooling.api.ResourceContainer;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.common.util.LogCommonConstants;

public abstract class BaseResource implements Resource {


    private String checkedOutBy;
    // indicates if the connection was returned back to the pool insteaad
    // of actually closed.  But to the user, if the connection has been
    // closed (i.e., returned to pool), the connection should no longer
    // be available
    private boolean isClosed = false;

/**
 * @label belongs to
 */
    private ResourceContainer container;

    // indicates the state of the resource, defautl to true, the
    // action of checkIsResourceAlive will set the lastest state 
    private boolean isAlive = true;

    protected static final String LOG_CONTEXT = LogCommonConstants.CTX_POOLING;

    public BaseResource() {
        this.checkedOutBy=null;
        this.container = null;
    }


	public void init(String checkedOutBy) throws ResourcePoolException {
		Assertion.isNotNull(checkedOutBy);
		
        this.checkedOutBy = checkedOutBy;
		
        // allow user to initialize the resource
        this.performInit();

//        this.isAlive = this.checkIsResourceAlive();
    // vah 5/20/03 no longer perform is alive because it hits the database
    // it will be assumed alive.  the pool will manage the alive state of
    // each resource
        this.isAlive = true;
        this.isClosed = false;
		
        
	}


    /**
     * This method is invoked by the pool to set a reference of itself on the resource
     * @param resourcePool is the resource pool from which the resource is managed.
     * @throws ResourcePoolException if an error occurs initializing resource.
     */

    public void init(ResourceContainer resourceContainer, String checkedOutBy) throws ResourcePoolException {
		init(checkedOutBy);
		
        this.container = resourceContainer;              
    }

    /**
     * Returns name of user that checked out this resource
     * @return String is the name of the user who checked out the resource
     */
    public String getCheckedOutBy() {
        return this.checkedOutBy;
    }
    
    public ResourceContainer getContainer() {
        return this.container;
    }
    

    /**
     * This method should be invoked by the extended class to notify the pool
     * the use of the resource is not longer needed.  This is where the resource
     * will be returned back to the pool for possible reuse.
     * @throws ResourcePoolConnection if there is an error closing the resource
     */
    public final synchronized void closeResource() throws ResourcePoolException {
         container.checkin(this, checkedOutBy);
         this.isClosed = true;
    }
    
    
    /**
     * Call isAlive to determine if the resource  is operating normally. This
     * method is called by the pool when the resource is checked in.
     * A return of  <code>false</code> indicates the resource should not be used.
     * If the resource is unusable, it will be shutdown an another
     * instance will take its place.
     * @return boolean true if the resource is operating normally
     */
    public final synchronized boolean isResourceAlive() {

        if (!this.isAlive) {
            return this.isAlive;
        }
        
        this.isAlive = checkIsResourceAlive();

        return this.isAlive;

    }

    public final boolean isClosed() throws SQLException {
        return this.isClosed;
    }


//************************************************************
//
//  A b s t r a c t    M e t h o d s
//
//************************************************************


    /**
     * Perform initialization allows for additional initialization by the
     * implementor before the resource object is made avaialable for use.
     * @throws ResourcePoolException if an error occurs initializing resource.
     */

    protected abstract void performInit() throws ResourcePoolException ;


    /**
     * This method is invoked when {@link #isAlive} is called to determine
     * if the resource is operating normally. This logic is called
     * by the pool when the resource is checked in to verify its state
     * prior to placing it back in the pool.
     * <p>
     * A return of  <code>false</code> indicates the resource should not be used.
     * If the resource is unusable, it will be shutdown an another
     * instance will take its place.
     * </p>
     * @return boolean true if the resource is operating normally
     */
    protected abstract boolean checkIsResourceAlive();

} 
