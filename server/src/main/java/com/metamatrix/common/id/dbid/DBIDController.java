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

package com.metamatrix.common.id.dbid;

public interface DBIDController {

    // call to set the block size increments for a given context.
    void setContextBlockSize(String context, long size);

    // call to get a unique id for the given context and by default
    // the id numbers cannot be rolled over and reused.
    long getUniqueID(String context) throws DBIDGeneratorException;

    // call to get a unique id for the given context and pass true if
    // the id numbers can be rolled over and reused.
    long getUniqueID(String context, boolean enableRollOver) throws DBIDGeneratorException;


    /**
    * Call when the DBIDGenerator is no longer needed and the database connections
    * can be closed.
    */
    void shutDown();


} 
