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

package com.metamatrix.platform.security.api;

import java.io.Serializable;
import java.util.Properties;
import java.util.TimeZone;



/**
 * Dataholder for the result of <code>ILogon.logon()</code>.
 * Contains a sessionID
 * 
 * Analogous to the server side SessionToken
 */
public class LogonResult implements Serializable {
        
    private MetaMatrixSessionID sessionID;
    private TimeZone timeZone = TimeZone.getDefault();
    private final Properties productInfo;
    private String userName;
    private long pingInterval;

    public LogonResult() {
		this.productInfo = new Properties();
	}
    
    public LogonResult(MetaMatrixSessionID sessionID,
			String userName, Properties productInfo, long pingInterval) {
		this.sessionID = sessionID;
		this.userName = userName;
		this.productInfo = productInfo;
		this.pingInterval = pingInterval;
	}

	/**
     * Get the sessionID. 
     * @return
     * @since 4.3
     */
    public MetaMatrixSessionID getSessionID() {
        return this.sessionID;
    }

	public TimeZone getTimeZone() {
		return timeZone;
	}
	
	public String getProductInfo(String key) {
		return this.productInfo.getProperty(key);
	}

	public String getUserName() {
		return userName;
	}

	public long getPingInterval() {
		return pingInterval;
	}
    
}
