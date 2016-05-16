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
package org.teiid.metadata;


public class RestMetadataExtension {
    public enum ParameterType {
        PATH,QUERY,FORM,FORMDATA,BODY,HEADER;
    }
    
    public final static String URI = "teiid_rest:URI";
    public final static String METHOD = "teiid_rest:METHOD";
    public final static String SCHEME = "teiid_rest:SCHEME";
    public final static String PRODUCES = "teiid_rest:PRODUCES";
    public final static String CONSUMES = "teiid_rest:CONSUMES";
    public final static String CHARSET = "teiid_rest:CHARSET";
    public final static String PARAMETER_TYPE = "teiid_rest:PARAMETER_TYPE";
    public final static String COLLECION_FORMAT = "teiid_rest:COLLECION_FORMAT";
}
