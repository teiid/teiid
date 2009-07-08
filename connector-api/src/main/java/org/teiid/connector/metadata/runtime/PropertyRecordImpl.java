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
 * PropertyRecordImpl
 */
public class PropertyRecordImpl extends AbstractMetadataRecord {

    /**
	 * Constants for names of accessor methods that map to fields stored  on the PropertyRecords.
	 * Note the names do not have "get" on them, this is also the nameInsource
	 * of the attributes on SystemPhysicalModel.
	 * @since 4.3
	 */
	public static interface MetadataFieldNames {
	    String PROPERTY_NAME_FIELD    = "PropertyName"; //$NON-NLS-1$
	    String PROPERTY_VALUE_FIELD    = "PropertyValue"; //$NON-NLS-1$
	}

	private String name;
    private String value;

    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.PropertyRecord#getPropertyName()
     */
    public String getPropertyName() {
        return this.name;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.PropertyRecord#getPropertyValue()
     */
    public String getPropertyValue() {
        return this.value;
    }

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append(getClass().getSimpleName());
        sb.append(", uuid="); //$NON-NLS-1$
        sb.append(getUUID());
        sb.append(" propName="); //$NON-NLS-1$
        sb.append(getPropertyName());
        sb.append(" propValue="); //$NON-NLS-1$
        sb.append(getPropertyValue());
        sb.append(", pathInModel="); //$NON-NLS-1$
        sb.append(getPath());
        return sb.toString();
    }

    /**
     * @param list
     */
    public void setPropertyName(final String name) {
        this.name = name;
    }

    /**
     * @param list
     */
    public void setPropertyValue(final String value) {
        this.value = value;
    }

}