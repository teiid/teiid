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
package org.teiid.rhq.embedded.pool;


/** 
 * These are the Constants that used when the jbedsp-plugin is running in the enbedded environment
 */
public interface EmbeddedConnectionConstants {
    
    /**
     * These are Environment properties need to create a connection.  They will be exposed via the @see #getEnvironment call.
     */
        public final static String USERNAME = "username"; //$NON-NLS-1$
        public final static String PASSWORD = "password"; //$NON-NLS-1$
        public final static String URL = "url"; //$NON-NLS-1$
        

        public final static String SYSTEM_KEY="jbedsp_system"; //$NON-NLS-1$
         
   }
