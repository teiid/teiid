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

package com.metamatrix.jdbc.api;

/**
 * Annotation from query engine, collected during execution.
 */
public interface Annotation {

    public static final int LOW = 1;
    public static final int MEDIUM = 2;
    public static final int HIGH = 3;

    /**
     * Get category of this annotation.
     * @return Category
     */
    public String getCategory();
    
    /**
     * Get annotation description.
     * @return Annotation description
     */
    public String getAnnotation();
    
    /**
     * Get resolution for annotation.  May be null if no resolution is suggested.
     * @return Resolution, or null
     */
    public String getResolution();
    
    /**
     * Get severity of this annotation
     * @return Severity level
     * @see #LOW
     * @see #MEDIUM
     * @see #HIGH
     */
    public int getSeverity();
}
