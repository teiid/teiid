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

package org.teiid.logging;

import java.util.Arrays;

import org.teiid.CommandContext;

/**
 * Log format for auditing.
 */
public class AuditMessage {
	private String context;
	private String activity;
	private String[] resources;
	private CommandContext commandContext;

	public AuditMessage(String context, String activity, String[] resources, CommandContext commandContext) {
	    this.context = context;
	    this.activity = activity;
	    this.resources = resources;
	    this.commandContext = commandContext;
	}

    public String getContext() {
        return this.context;
    }

    public String getActivity() {
        return this.activity;
    }

    public String getPrincipal() {
        return this.commandContext.getUserName();
    }

	public String[] getResources() {
		return this.resources;
	}
	
	public CommandContext getCommandContext() {
		return commandContext;
	}
	
	public String toString() {
        StringBuffer msg = new StringBuffer();
        msg.append( this.commandContext.getRequestId());
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
