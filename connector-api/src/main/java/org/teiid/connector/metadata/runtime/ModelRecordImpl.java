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

package org.teiid.connector.metadata.runtime;


/**
 * ModelRecordImpl
 */
public class ModelRecordImpl extends AbstractMetadataRecord {

	public enum Type {
		Physical,
		Virtual,
		Type,
		VDB_Archive() {
			@Override
			public String toString() {
				return "VDB Archive"; //$NON-NLS-1$
			}
		},
		Unknown,
		Function,
		Configuration,
		Metamodel,
		Extension,
		Logical,
		Materialization
	}
	
    private Type modelType;
    private boolean isVisible = true;
    private String primaryMetamodelUri;

    public String getPrimaryMetamodelUri() {
        return primaryMetamodelUri;
    }

    public boolean isVisible() {
        return isVisible;
    }

    public Type getModelType() {
        return modelType;
    }

    public boolean isPhysical() {
        return getModelType() == Type.Physical;
    }

    /**
     * @param string
     */
    public void setPrimaryMetamodelUri(String string) {
        primaryMetamodelUri = string;
    }

    /**
     * @param b
     */
    public void setVisible(boolean b) {
        isVisible = b;
    }

    /**
     * @param i
     */
    public void setModelType(Type i) {
        modelType = i;
    }

}
