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

package com.metamatrix.metadata.runtime;

import org.teiid.metadata.index.IndexConstants;

import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.unittest.QueryMetadataInterfaceBuilder;

public class FakeQueryMetadata {
	private static QueryMetadataInterface metadata;
	
    public static QueryMetadataInterface getQueryMetadata() {
        if (metadata == null) {
            QueryMetadataInterfaceBuilder builder = new QueryMetadataInterfaceBuilder();
            builder.addPhysicalModel("system"); //$NON-NLS-1$
            
            builder.addGroup("tables", IndexConstants.INDEX_NAME.TABLES_INDEX + "#B"); //$NON-NLS-1$ //$NON-NLS-2$
            builder.addElement("FullName", String.class); //$NON-NLS-1$
            builder.addElement("Path", String.class); //$NON-NLS-1$
            //builder.addElement("UUID", String.class);
            builder.addElement("Cardinality", Integer.class);      //$NON-NLS-1$
            builder.addElement("supportsUpdate", Boolean.class);      //$NON-NLS-1$
            
            builder.addGroup("columns", IndexConstants.INDEX_NAME.COLUMNS_INDEX + "#G"); //$NON-NLS-1$ //$NON-NLS-2$
            builder.addElement("FullName", String.class); //$NON-NLS-1$
            
            builder.addGroup("models", IndexConstants.INDEX_NAME.MODELS_INDEX + "#A"); //$NON-NLS-1$ //$NON-NLS-2$
            builder.addElement("FullName", String.class); //$NON-NLS-1$
            builder.addElement("MaxSetSize", Integer.class); //$NON-NLS-1$
            
            builder.addGroup("foreignKeys", IndexConstants.INDEX_NAME.KEYS_INDEX + "#J"); //$NON-NLS-1$ //$NON-NLS-2$
            builder.addElement("FullName", String.class); //$NON-NLS-1$
            
            builder.addGroup("primaryKeys", IndexConstants.INDEX_NAME.KEYS_INDEX + "#K"); //$NON-NLS-1$ //$NON-NLS-2$
            builder.addElement("FullName", String.class); //$NON-NLS-1$
            
            builder.addGroup("procs", IndexConstants.INDEX_NAME.PROCEDURES_INDEX + "#E"); //$NON-NLS-1$ //$NON-NLS-2$
            builder.addElement("Name", String.class); //$NON-NLS-1$
            
            builder.addGroup("junk", "junk"); //$NON-NLS-1$ //$NON-NLS-2$
            builder.addElement("UUID", String.class); //$NON-NLS-1$
            builder.addElement("toString", String.class); //$NON-NLS-1$
            
            builder.addGroup("fake1Properties", "DatatypeTypeEnumeration.properties"); //$NON-NLS-1$ //$NON-NLS-2$
            builder.addElement("Key", Integer.class); //$NON-NLS-1$
            builder.addElement("Value", String.class); //$NON-NLS-1$
            
            metadata = builder.getQueryMetadata();
        }
        return metadata;
    }
}
