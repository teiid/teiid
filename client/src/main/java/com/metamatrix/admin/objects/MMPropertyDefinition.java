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
import java.util.Collection;

import org.teiid.adminapi.PropertyDefinition;

import com.metamatrix.admin.AdminPlugin;


/** 
 * @since 4.3
 */
public class MMPropertyDefinition extends MMAdminObject implements PropertyDefinition {

    private String value = null;
    private Collection allowedValues = new ArrayList();
    private Object defaultValue = null;
    private String description = null;
    private String displayName = null;
    private String propertyType = "String"; //$NON-NLS-1$
    private String propertyTypeClassName = String.class.getName();
    private RestartType requiresRestart = RestartType.NONE;
    private boolean expert = false;
    private boolean masked = false;
    private boolean modifiable = true;
    private boolean required = false;
    
    
    
    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuffer result = new StringBuffer();
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.MMPropertyDefinition")).append(getIdentifier()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Display_name")).append(getDisplayName()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Description")).append(getDescription()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Value")).append(getValue()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Property_type")).append(getPropertyType()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Property_type_class_name")).append(getPropertyTypeClassName()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Default_value")).append(getDefaultValue()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Constrained_to_allow_values")).append(isConstrainedToAllowedValues()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Allowed_values")).append(getAllowedValues()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Required")).append(isRequired()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Expert")).append(isExpert()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Masked")).append(isMasked()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.Modifiable")).append(isModifiable()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMPropertyDefinition.RequiresRestart")).append(getRequiresRestart()); //$NON-NLS-1$
        return result.toString();
    }
    
    
    
    
    /**
     * Constructor.
     * @param identifierParts
     * @since 4.3
     */
    public MMPropertyDefinition(String[] identifierParts) {
        super(identifierParts);
    }
    
    
    
    
    /** 
     * @see org.teiid.adminapi.PropertyDefinition#getValue()
     * @since 4.3
     */
    public String getValue() {
        return value;
    }

    /** 
     * @see org.teiid.adminapi.PropertyDefinition#getAllowedValues()
     * @since 4.3
     */
    public Collection getAllowedValues() {
        return allowedValues;
    }
    
    /** 
     * @see org.teiid.adminapi.PropertyDefinition#getDefaultValue()
     * @since 4.3
     */
    public Object getDefaultValue() {
        return defaultValue;
    }

    /** 
     * @see org.teiid.adminapi.PropertyDefinition#getDescription()
     * @since 4.3
     */
    public String getDescription() {
        return description;
    }

    /** 
     * @see org.teiid.adminapi.PropertyDefinition#getDisplayName()
     * @since 4.3
     */
    public String getDisplayName() {
        return displayName;
    }

    /** 
     * @see org.teiid.adminapi.PropertyDefinition#getPropertyType()
     * @since 4.3
     */
    public String getPropertyType() {
        return propertyType;
    }
    
    /** 
     * @see org.teiid.adminapi.PropertyDefinition#getPropertyTypeClassName()
     * @since 4.3
     */
    public String getPropertyTypeClassName() {
        return propertyTypeClassName;
    }


    /** 
     * @see org.teiid.adminapi.PropertyDefinition#getRequiresRestart()
     * @since 4.3
     */
    public RestartType getRequiresRestart() {
        return requiresRestart;
    }

    /** 
     * @see org.teiid.adminapi.PropertyDefinition#isExpert()
     * @since 4.3
     */
    public boolean isExpert() {
        return expert;
    }

    /** 
     * @see org.teiid.adminapi.PropertyDefinition#isMasked()
     * @since 4.3
     */
    public boolean isMasked() {
        return masked;
    }

    /** 
     * @see org.teiid.adminapi.PropertyDefinition#isModifiable()
     * @since 4.3
     */
    public boolean isModifiable() {
        return modifiable;
    }

    /** 
     * @see org.teiid.adminapi.PropertyDefinition#isRequired()
     * @since 4.3
     */
    public boolean isRequired() {
        return required;
    }




    
    
    /** 
     * @param allowedValues The allowedValues to set.
     * @since 4.3
     */
    public void setAllowedValues(Collection allowedValues) {
        this.allowedValues = allowedValues;
    }

    /** 
     * @param defaultValue The defaultValue to set.
     * @since 4.3
     */
    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    /** 
     * @param description The description to set.
     * @since 4.3
     */
    public void setDescription(String description) {
        this.description = description;
    }

    
    /** 
     * @param displayName The displayName to set.
     * @since 4.3
     */
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    
    /** 
     * @param expert The value of expert to set.
     * @since 4.3
     */
    public void setExpert(boolean expert) {
        this.expert = expert;
    }

    /** 
     * @param masked The value of masked to set.
     * @since 4.3
     */
    public void setMasked(boolean masked) {
        this.masked = masked;
    }
    /** 
     * @param modifiable The value of modifiable to set.
     * @since 4.3
     */
    public void setModifiable(boolean modifiable) {
        this.modifiable = modifiable;
    }

    /** 
     * @param propertyTypeAsString The propertyTypeAsString to set.
     * @since 4.3
     */
    public void setPropertyType(String propertyTypeAsString) {
        this.propertyType = propertyTypeAsString;
    }
    
    /** 
     * @param propertyTypeClassName The propertyTypeName to set.
     * @since 4.3
     */
    public void setPropertyTypeClassName(String propertyTypeClassName) {
        this.propertyTypeClassName = propertyTypeClassName;
    }
    
    
    /** 
     * @param required The value of required to set.
     * @since 4.3
     */
    public void setRequired(boolean required) {
        this.required = required;
    }
    
    /** 
     * @param requiresRestart The value of requiresRestart to set.
     * @since 4.3
     */
    public void setRequiresRestart(RestartType requiresRestart) {
        this.requiresRestart = requiresRestart;
    }

    /** 
     * @param value The value to set.
     * @since 4.3
     */
    public void setValue(String value) {
        this.value = value;
    }

	@Override
	public boolean isConstrainedToAllowedValues() {
		return allowedValues != null && !allowedValues.isEmpty();
	}
    
    
}
