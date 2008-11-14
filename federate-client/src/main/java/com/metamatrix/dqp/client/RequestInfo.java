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

import java.io.Serializable;


/** 
 * Represents all information needed to execute a request on the server.
 * @since 4.3
 */
public interface RequestInfo {
    
    public static final int REQUEST_TYPE_STATEMENT = 0;
    public static final int REQUEST_TYPE_PREPARED_STATEMENT = 1;
    public static final int REQUEST_TYPE_CALLABLE_STATEMENT = 2;
    
    public static final int AUTOWRAP_OFF = 0;
    public static final int AUTOWRAP_ON = 1;
    public static final int AUTOWRAP_OPTIMISTIC = 2;
    public static final int AUTOWRAP_PESSIMISTIC = 3;

    /**
     * Set the SQL string 
     * @param sql SQL string
     * @since 4.3
     */
    void setSql(String sql);
    
    /**
     * Type of request
     * @param type Statement / prepared statement / callable statement / ???
     * @since 4.3
     */
    void setRequestType(int type);

    /**
     * Set bind parameter values 
     * @param params Parameter values
     * @since 4.3
     */
    void setBindParameters(Object[] params);
    
    /**
     * Cursor type for requests that return a cursor - forward-only or scroll-insensitive 
     * @param type Cursor type
     * @since 4.3
     */
    void setCursorType(int type);
    
    /**
     * Fetch size for cursored batches. 
     * @param size Fetch size
     * @since 4.3
     */
    void setFetchSize(int size);
    
    /**
     * Set whether partial results mode is on or off 
     * @param flag 
     * @since 4.3
     */
    void setPartialResults(boolean flag);
    
    /**
     * Set whether XML schema validation is on or off 
     * @param flag
     * @since 4.3
     */
    void setXMLValidationMode(boolean flag);
    
    /**
     * Set XML results format (formatted tree or raw string), default is determined by model 
     * @param format
     * @since 4.3
     */
    void setXMLFormat(String format);
    
    /**
     * Set XSLT style sheet to use (only for XML docs)
     * @param styleSheet Stylesheet text
     * @since 4.3
     */
    void setXMLStyleSheet(String styleSheet);
    
    /**
     * Set transaction auto wrap mode. 
     * @param autoWrapMode
     * @since 4.3
     */
    void setTransactionAutoWrapMode(int autoWrapMode);
    
    /**
     * Set whether to use result set caching or not. 
     * @param flag
     * @since 4.3
     */
    void setUseResultSetCache(boolean flag);
    
    /**
     * Set the Serializable per-command payload to use. 
     * @param payload
     * @since 4.3
     */
    void setCommandPayload(Serializable payload);
}
