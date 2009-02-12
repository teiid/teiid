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

package com.metamatrix.query.analysis;

/**
 * Annotation describing a decision made during query execution.
 */
public class QueryAnnotation {
    
    public static final String MATERIALIZED_VIEW = "Materialized View"; //$NON-NLS-1$
    
    public static final int LOW = 1;
    public static final int MEDIUM = 2;
    public static final int HIGH = 3;
    
    private String category;
    private String annotation;
    private String resolution;
    private int priority = LOW;
    
    public QueryAnnotation(String category, String annotation, String resolution, int priority) {
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
    
    public int getPriority() {
        return this.priority;
    }
    
    public String toString() {
        return "QueryAnnotation<" + getCategory() + ", " + getAnnotation() + ">";  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }
}
