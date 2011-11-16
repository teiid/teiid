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

package org.teiid.services;

import javax.security.auth.Subject;


/**
 * This class serves as the primary implementation of the
 * Membership Service. Based on the security domains specified this class delegates the responsibility of
 * authenticating user to those security domains in the order they are defined.
 */
public class TeiidLoginContext {
	
	private Subject subject;
	private String userName;
	private String securitydomain;
	private Object securityContext;
	
	public TeiidLoginContext(String userName, Subject subject, String securityDomain, Object sc) {
		this.userName = userName;
		this.subject = subject;
		this.securitydomain = securityDomain;
		this.securityContext = sc;
	}
	    
    public String getUserName() {
    	return this.userName;
    }
    
    public String getSecurityDomain() {
    	return this.securitydomain;
    }
    
    public Subject getSubject() {
    	return this.subject;
    }
    
    public Object getSecurityContext() {
    	return this.securityContext;
    }
}
