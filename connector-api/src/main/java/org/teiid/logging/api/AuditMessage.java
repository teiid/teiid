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

package org.teiid.logging.api;

import java.util.Arrays;

/**
 * Log format for auditing.
 */
public class AuditMessage {
	private String context;
	private String activity;
	private String principal;
	private Object[] resources;

	public AuditMessage(String context, String activity, String principal, String[] resources ) {
	    this.context = context;
	    this.activity = activity;
	    this.principal = principal;
	    this.resources = resources;
	}

    public String getContext() {
        return this.context;
    }

    public String getActivity() {
        return this.activity;
    }

    public String getPrincipal() {
        return this.principal;
    }

	public Object[] getResources() {
		return this.resources;
	}

	public String toString() {
        StringBuffer msg = new StringBuffer();
        msg.append(" ["); //$NON-NLS-1$
        msg.append( getPrincipal() );
        msg.append("] <"); //$NON-NLS-1$
        msg.append( getContext() );
        msg.append('.');
        msg.append( getActivity() );
        msg.append("> "); //$NON-NLS-1$
    	msg.append( Arrays.toString(resources) );
        return msg.toString();
	}

}
