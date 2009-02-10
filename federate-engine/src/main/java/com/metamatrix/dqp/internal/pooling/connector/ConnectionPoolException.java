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
package com.metamatrix.dqp.internal.pooling.connector;

import com.metamatrix.connector.exception.ConnectorException;

/**
 * This class is for exceptions occurring within the connection pool.
 */
public class ConnectionPoolException extends ConnectorException{


    /**
     * No-arg constructor required by Externalizable semantics.
     */
    public ConnectionPoolException() {
        super();
    }
    
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public ConnectionPoolException( String message ) {
        super( message );
    }

    /**
     * Construct an instance with a linked exception specified.
     *
     * @param e An exception to chain to this exception
     */
    public ConnectionPoolException( Throwable e ) {
        super( e );
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public ConnectionPoolException( Throwable e, String message ) {
        super( e, message );
    }

}
