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
package org.teiid.translator.couchbase;

public interface CouchbaseProperties {
    
    String WAVE = "`"; //$NON-NLS-1$
    String COLON = ":"; //$NON-NLS-1$
    String QUOTE = "'"; //$NON-NLS-1$
    String SOURCE_SEPARATOR = "."; //$NON-NLS-1$
    String PLACEHOLDER = "$"; //$NON-NLS-1$
    String JSON = "json"; //$NON-NLS-1$ 
    String PK = "PK"; //$NON-NLS-1$
    String UNDERSCORE = "_"; //$NON-NLS-1$
    String IDX_SUFFIX = "_idx"; //$NON-NLS-1$
    String DIM_SUFFIX = "dim"; //$NON-NLS-1$
    String SQUARE_BRACKETS = "[]"; //$NON-NLS-1$
    
    String TRUE_VALUE = "true"; //$NON-NLS-1$
    String FALSE_VALUE = "false"; //$NON-NLS-1$
    
    String UNNEST = "UNNEST"; //$NON-NLS-1$
    String UNNEST_POSITION = "UNNEST_POSITION"; //$NON-NLS-1$
    String LET = "LET"; //$NON-NLS-1$
    
    String DEFAULT_NAMESPACE = "default"; //$NON-NLS-1$
    String DEFAULT_TYPENAME = "type"; //$NON-NLS-1$
    String TPYENAME_MATCHER_PATTERN = "([a-zA-Z_]\\w*|(?:`[^`]*`)+):([a-zA-Z_]\\w*|(?:`[^`]*`)+)(?:$|,)"; //$NON-NLS-1$

    String NAME = "name"; //$NON-NLS-1$
    String DOCUMENTID = "documentID"; //$NON-NLS-1$
    
    // Procedure Name
    String GETTEXTDOCUMENTS = "getTextDocuments"; //$NON-NLS-1$
    String GETDOCUMENTS = "getDocuments"; //$NON-NLS-1$
    String GETTEXTDOCUMENT = "getTextDocument"; //$NON-NLS-1$
    String GETDOCUMENT = "getDocument"; //$NON-NLS-1$
    String SAVEDOCUMENT = "saveDocument"; //$NON-NLS-1$
    String DELETEDOCUMENT = "deleteDocument"; //$NON-NLS-1$
    String GETMETADATADOCUMENT  = "getMetadataDocument"; //$NON-NLS-1$
    String GETTEXTMETADATADOCUMENT  = "getTextMetadataDocument"; //$NON-NLS-1$
    
    // Procedure Parameter Name
    String ID = "id"; //$NON-NLS-1$
    String RESULT = "result"; //$NON-NLS-1$
    String KEYSPACE = "keyspace"; //$NON-NLS-1$
    String DOCUMENT = "document"; //$NON-NLS-1$
    
    String N1QL_COLUMN_ALIAS_PREFIX = "$cb_c"; //$NON-NLS-1$
    String N1QL_TABLE_ALIAS_PREFIX = "$cb_t"; //$NON-NLS-1$

}
