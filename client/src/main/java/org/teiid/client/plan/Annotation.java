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

package org.teiid.client.plan;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Annotation describing a decision made during query execution.
 */
public class Annotation implements Externalizable {
    
	public static final String MATERIALIZED_VIEW = "Materialized View"; //$NON-NLS-1$
	public static final String CACHED_PROCEDURE = "Cached Procedure"; //$NON-NLS-1$
    public static final String HINTS = "Hints"; //$NON-NLS-1$
    public static final String RELATIONAL_PLANNER = "Relational Planner"; //$NON-NLS-1$
    
    public enum Priority {
		LOW,
		MEDIUM,
		HIGH
    }
    
    private String category;
    private String annotation;
    private String resolution;
    private Priority priority = Priority.LOW;
    
    public Annotation() {
    	
    }
    
    public Annotation(String category, String annotation, String resolution, Priority priority) {
        this.category = category;
        this.annotation = annotation;
        this.resolution = resolution;
        this.priority = priority;
    }
    
    public String getCategory() {
        return this.category;
    }
    
    public String getAnnotation() {
        return this.annotation;
    }
    
    public String getResolution() {
        return this.resolution;
    }
    
    public Priority getPriority() {
        return this.priority;
    }
    
    public String toString() {
        return "QueryAnnotation<" + getCategory() + ", " + getPriority() + "," + getAnnotation() + "," + getResolution() + ">";  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException,
    		ClassNotFoundException {
    	annotation = (String)in.readObject();
    	category = (String)in.readObject();
    	resolution = (String)in.readObject();
    	priority = Priority.values()[in.readByte()];
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
    	out.writeObject(annotation);
    	out.writeObject(category);
    	out.writeObject(resolution);
    	out.writeByte(priority.ordinal());
    }
}
