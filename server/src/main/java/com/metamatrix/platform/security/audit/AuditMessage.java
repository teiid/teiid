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

package com.metamatrix.platform.security.audit;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.metamatrix.common.config.CurrentConfiguration;

public class AuditMessage implements Externalizable {
    public static final String PROCESS_NAME = CurrentConfiguration.getInstance().getProcessName();
    public static final String HOST_NAME = CurrentConfiguration.getInstance().getConfigurationName();

    private static final String RESOURCE_DELIMITER = ", "; //$NON-NLS-1$

	private String context;
	private String activity;
	private String principal;
    //private int level;
	private Object[] resources;
	private long timestamp;
    //private String threadName;
    private String hostName;
    private String processName;

	public AuditMessage() {
	}

	public AuditMessage(String context, String activity, String principal, Object[] resources ) {
	    this.context = context;
	    this.activity = activity;
	    this.principal = principal;
	    //this.level = level;
	    this.resources = resources;
	    this.timestamp = System.currentTimeMillis();
        //this.threadName = Thread.currentThread().getName();
        this.hostName = HOST_NAME;
        this.processName = PROCESS_NAME;
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

    public String getProcessName() {
        return this.processName;
    }

    public String getHostName() {
        return this.hostName;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

	public Object[] getResources() {
		return this.resources;
	}

	public String getText() {
		StringBuffer text = new StringBuffer();
		if(resources != null && resources.length > 0 ) {
            Object resource = resources[0];
            if ( resource != null ) {
                text.append(resource.toString());
            }
		    for(int i=1; i<resources.length; ++i) {
		        text.append(RESOURCE_DELIMITER);
                resource = resources[i];
                if ( resource != null ) {
                    text.append(resource.toString());
                }
		    }
		}
		return text.toString();	
	}

	public String getText( String delimiter ) {
        StringBuffer text = new StringBuffer();
        if ( delimiter != null ) {
            if(resources != null && resources.length > 0 ) {
                Object resource = resources[0];
                if ( resource != null ) {
                    text.append(resource.toString());
                }
                for(int i=1; i<resources.length; ++i) {
                    text.append(delimiter);
                    resource = resources[i];
                    if ( resource != null ) {
                        text.append(resource.toString());
                    }
                }
            }
        } else {
            if(resources != null && resources.length > 0 ) {
                Object resource = resources[0];
                if ( resource != null ) {
                    text.append(resource.toString());
                }
                for(int i=1; i<resources.length; ++i) {
                    text.append(RESOURCE_DELIMITER);
                    resource = resources[i];
                    if ( resource != null ) {
                        text.append(resource.toString());
                    }
                }
            }
        }
		return text.toString();
	}

	// implements Externalizable
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(context);
		out.writeObject(activity);
		out.writeObject(principal);
		//out.writeInt(level);
		out.writeObject(resources);
		out.writeLong(timestamp);
//		out.writeObject(threadName);
		out.writeObject(processName);
	}

	// implements Externalizable
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.context = (String) in.readObject();
		this.activity = (String) in.readObject();
		this.principal = (String) in.readObject();
		//this.level = in.readInt();
		this.resources = (Object[]) in.readObject();
		this.timestamp = in.readLong();
//		this.threadName = (String) in.readObject();
		this.processName = (String) in.readObject();
	}

}
