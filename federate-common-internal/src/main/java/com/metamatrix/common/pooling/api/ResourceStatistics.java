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

package com.metamatrix.common.pooling.api;



/**
* ResourceStatistics track the connection stats for a specific
* Resource instance so the ResoucePool knows
*/
public final class ResourceStatistics implements java.io.Serializable {
    private int concurrentUsers;
    private long lastUsed;
    private String userName=""; //$NON-NLS-1$
    private long creationTime = System.currentTimeMillis();

    public ResourceStatistics() {
        this.concurrentUsers = 0;

    }

    /**
    * Returns the number of users currently using the resource
    * @return int is the number of current users
    */
    public synchronized int getConcurrentUserCount() {
        return concurrentUsers;
    }

    /**
    * Returns <code>true</code> if the resource has current users
    * @return boolean of true if the resource has current users
    */
    public synchronized boolean hasConcurrentUsers() {
        return this.concurrentUsers > 0;
    }

    /**
    * Adds a user to the current list of current users of the resource
    * @param userName
    */
    public synchronized void addConcurrentUser(String userName) {
       ++this.concurrentUsers;
        this.markAsUsed();
        this.userName=userName;
    }

    public synchronized void removeConcurrentUser(String userName) {
        this.userName=""; //$NON-NLS-1$
        if ( this.concurrentUsers > 0 ) {
            --this.concurrentUsers;
        }
    }

    public long getCreationTime() {
        return creationTime;
    }

    public synchronized long getLastUsed() {
        return this.lastUsed;
    }

    public synchronized void markAsUsed() {
        this.lastUsed = System.currentTimeMillis();
    }

    public String getUserName() {
        return userName;
    }
}
