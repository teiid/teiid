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

package com.metamatrix.dqp.client;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;


/** 
 * Represents low-level access to the server.
 * @since 4.3
 */
public interface ServerFacade {

    /**
     * Connect to the server and return a stringable object that can be used
     * to represent the server session.  This session context can be used in a 
     * different VM to reconnect to the existing session.  
     * @param connectionInfo Information needed to connect
     * @return Session context, stringable for later reconnection
     * @throws MetaMatrixComponentException If the operation fails due to an internal error
     * @throws MetaMatrixProcessingException If the operation fails due to invalid user input
     * @since 4.3
     */
    PortableContext createSession(ConnectionInfo connectionInfo) throws MetaMatrixComponentException, MetaMatrixProcessingException;
    
    /**
     * Execution a request and return a stringable object that can be used
     * to represent the request context.  This request context can be used in a 
     * different VM to reconnect to the existing request state (in combination with 
     * the session context).  
     * @param connectionContext Session context, used to reconnect to the same session
     * @param requestInfo Request information
     * @return Request context, used to reconnect to the same request state
     * @throws MetaMatrixComponentException If the operation fails due to an internal error
     * @throws MetaMatrixProcessingException If the operation fails due to invalid user input
     * @since 4.3
     */
    PortableContext executeRequest(PortableContext connectionContext, RequestInfo requestInfo) throws MetaMatrixComponentException, MetaMatrixProcessingException;
    
    /**
     * Get some results for the specified request.  This method will wait for up to waitTime
     * to send the results.  If none are available within the waitTime, null is returned.
     * <br/>Constraints:
     * <br/>1) 1 &lt;= beginRow &lt;= maxEndRow : This API only supports forward retrieval of result data
     * 
     * @param connectionContext Session context, used to reconnect to the same session
     * @param requestContext Request context, used to reconnect to the request state
     * @param beginRow Requested begin row of batch, ignored if no result set exists
     * @param maxEndRow Requested max end row of batch (may get less), ignored if no result set exists
     * @param waitTime Wait time (in milliseconds) to wait for results
     * @return Results containing a batch of data or output parameters OR null if timeout occurs
     * @throws MetaMatrixComponentException If the operation fails due to an internal error
     * @throws MetaMatrixProcessingException If the operation fails due to invalid user input
     * @since 4.3
     */
    Results getBatch(PortableContext connectionContext, PortableContext requestContext, int beginRow, int maxEndRow, int waitTime) throws MetaMatrixComponentException, MetaMatrixProcessingException;
    
    /**
     * Get request metadata  
     * @param connectionContext Session context, used to reconnect to the same session
     * @param requestContext Request context, used to reconnect to the request state
     * @throws MetaMatrixComponentException If the operation fails due to an internal error
     * @throws MetaMatrixProcessingException If the operation fails due to invalid user input
     * @since 4.3
     */
    ResultsMetadata getMetadata(PortableContext connectionContext, PortableContext requestContext) throws MetaMatrixComponentException, MetaMatrixProcessingException;
    
    /**
     * Cancel an executing request. If there is a pending getBatch() request, that method will return
     * (almost) immediately, and the returned Result from that method will contain an exception notifying
     * the user that the request was cancelled.
     * @param connectionContext Session context, used to reconnect to the same session
     * @param requestContext Request context, used to reconnect to the request state
     * @throws MetaMatrixComponentException If the operation fails due to an internal error
     * @throws MetaMatrixProcessingException If the operation fails due to invalid user input
     * @since 4.3
     */
    void cancelRequest(PortableContext connectionContext, PortableContext requestContext) throws MetaMatrixComponentException, MetaMatrixProcessingException;

    /**
     * Close the request context. This method is expected to be called when there are no pending calls on the
     * request context. If there is a pending getBatch() request, please call cancelRequest() before calling
     * closeRequest().
     * @param connectionContext Session context, used to reconnect to the same session
     * @param requestContext Request context, used to reconnect to the request state
     * @throws MetaMatrixComponentException If the operation fails due to an internal error
     * @throws MetaMatrixProcessingException If the operation fails due to invalid user input
     * @since 4.3
     */
    void closeRequest(PortableContext connectionContext, PortableContext requestContext) throws MetaMatrixComponentException, MetaMatrixProcessingException;
    
    /**
     * Close the session state 
     * @param connectionContext Session context, used to reconnect to the same session
     * @since 4.3
     * @throws MetaMatrixComponentException If the operation fails due to an internal error
     * @throws MetaMatrixProcessingException If the operation fails due to invalid user input
     */
    void closeSession(PortableContext connectionContext) throws MetaMatrixComponentException, MetaMatrixProcessingException;
}
