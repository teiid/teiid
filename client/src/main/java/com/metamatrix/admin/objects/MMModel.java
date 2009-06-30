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

package com.metamatrix.admin.objects;

import java.util.ArrayList;
import java.util.List;

import org.teiid.adminapi.Model;

import com.metamatrix.admin.AdminPlugin;

/**
 */
public class MMModel extends MMAdminObject implements Model {
    
    private List connectorBindingNames = new ArrayList(); 
    private String modelType = ""; //$NON-NLS-1$
	private String modelURI = ""; //$NON-NLS-1$
    private boolean isMaterialization = false;
    private boolean isPhysical = false;
    private boolean isVisible = false;
    private boolean supportsMultiSourceBindings = false;
    
    
    
    /**
     * Construct a new MMModel
     * @param identifierParts
     */
    public MMModel(String[] identifierParts) {
        super(identifierParts);
    }
    

    /**
     * @see java.lang.Object#toString()
     * @return String for display purposes
     */
	public String toString() {

		StringBuffer result = new StringBuffer();
		result.append(AdminPlugin.Util.getString("MMModel.MMModel")).append(getIdentifier()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMModel.type")).append(getModelType()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMModel.physical")).append(isPhysical); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMModel.visible")).append(isVisible); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMModel.connector_bindings")).append(getConnectorBindingNames()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMModel.modelURI")).append(getModelURI()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMModel.properties")).append(getPropertiesAsString()); //$NON-NLS-1$
		return result.toString();
	}

   /** 
 * @see org.teiid.adminapi.Model#getConnectorBindingNames()
 * @since 4.3
 */
	public List getConnectorBindingNames() {
		return connectorBindingNames;
	}

	/** 
     * @see org.teiid.adminapi.Model#isPhysical()
     * @since 4.3
     */
	public boolean isPhysical() {
		return isPhysical;
	}

	/** 
     * @see org.teiid.adminapi.Model#isVisible()
     * @since 4.3
     */ 
	public boolean isVisible() {
		return isVisible;
	}

	/** 
     * @see org.teiid.adminapi.Model#getModelType()
     * @since 4.3
     */
	public String getModelType() {
		return modelType;
	}

	/** 
     * @see org.teiid.adminapi.Model#getModelURI()
     * @since 4.3
     */
	public String getModelURI() {
		return modelURI;
	}

    /** 
     * @see org.teiid.adminapi.Model#supportsMultiSourceBindings()
     * @since 4.3
     */
    public boolean supportsMultiSourceBindings() {
        return this.supportsMultiSourceBindings;
    }    
        
	/**
	 * @param bindings
	 */
	public void setConnectorBindingNames(List bindings) {
		connectorBindingNames = bindings;
	}
    
    /** 
     * @param supports
     * @since 4.3
     */
    public void setSupportsMultiSourceBindings(boolean supports) {
        this.supportsMultiSourceBindings = supports;
    }

 
  
    /** 
     * @param isPhysical Whether this model is visible.
     * @since 4.3
     */
    public void setPhysical(boolean isPhysical) {
        this.isPhysical = isPhysical;
    }

    
    /** 
     * @param isVisible Whether this model is visible.
     * @since 4.3
     */
    public void setVisible(boolean isVisible) {
        this.isVisible = isVisible;
    }

    
    /** 
     * @param modelType The modelType to set.
     * @since 4.3
     */
    public void setModelType(String modelType) {
        this.modelType = modelType;
    }

    
    /** 
     * @param modelURI The modelURI to set.
     * @since 4.3
     */
    public void setModelURI(String modelURI) {
        this.modelURI = modelURI;
    }

    
    /** 
     * @return Returns whether the model is a materialization.
     * @since 4.3
     */
    public boolean isMaterialization() {
        return this.isMaterialization;
    }

    
    /** 
     * @param isMaterialization whether the model is a materialization..
     * @since 4.3
     */
    public void setMaterialization(boolean isMaterialization) {
        this.isMaterialization = isMaterialization;
    }      

}
