/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.adminapi.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.teiid.adminapi.PropertyDefinition;



public class PropertyDefinitionMetadata extends AdminObjectImpl implements PropertyDefinition {
    private static final long serialVersionUID = 6612838530524627205L;
    private Collection allowedValues = new ArrayList();
    private Object defaultValue = null;
    private String description = null;
    private String displayName = null;
    private String propertyTypeClassName = String.class.getName();
    private RestartType requiresRestart = RestartType.NONE;
    private boolean advanced = false;
    private boolean masked = false;
    private boolean modifiable = true;
    private boolean required = false;
    private String category;

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuffer result = new StringBuffer();
        result.append("Display Name:").append(getDisplayName()); //$NON-NLS-1$
        result.append(" Name:").append(getName()); //$NON-NLS-1$
        result.append(" Description:").append(getDescription()); //$NON-NLS-1$
        result.append(" Property Type Classname:").append(getPropertyTypeClassName()); //$NON-NLS-1$
        result.append(" Default Value:").append(getDefaultValue()); //$NON-NLS-1$
        result.append(" Constrained To Allow Values:").append(isConstrainedToAllowedValues()); //$NON-NLS-1$
        result.append(" Allowed Values:").append(getAllowedValues()); //$NON-NLS-1$
        result.append(" Required:").append(isRequired()); //$NON-NLS-1$
        result.append(" Expert:").append(isAdvanced()); //$NON-NLS-1$
        result.append(" Masked:").append(isMasked()); //$NON-NLS-1$
        result.append(" Category:").append(getCategory()); //$NON-NLS-1$
        result.append(" Modifiable:").append(isModifiable()); //$NON-NLS-1$
        result.append(" RequiresRestart:").append(getRequiresRestart()); //$NON-NLS-1$
        return result.toString();
    }

    /**
     * @see org.teiid.adminapi.PropertyDefinition#getAllowedValues()
     */
    public Collection getAllowedValues() {
        return allowedValues;
    }

    /**
     * @see org.teiid.adminapi.PropertyDefinition#getDefaultValue()
     */
    public Object getDefaultValue() {
        return defaultValue;
    }

    /**
     * @see org.teiid.adminapi.PropertyDefinition#getDescription()
     */
    public String getDescription() {
        return description;
    }

    /**
     * @see org.teiid.adminapi.PropertyDefinition#getDisplayName()
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * @see org.teiid.adminapi.PropertyDefinition#getPropertyTypeClassName()
     */
    public String getPropertyTypeClassName() {
        return propertyTypeClassName;
    }


    /**
     * @see org.teiid.adminapi.PropertyDefinition#getRequiresRestart()
     */
    public RestartType getRequiresRestart() {
        return requiresRestart;
    }

    /**
     * @see org.teiid.adminapi.PropertyDefinition#isAdvanced()
     * @since 4.3
     */
    public boolean isAdvanced() {
        return advanced;
    }

    /**
     * @see org.teiid.adminapi.PropertyDefinition#isMasked()
     */
    public boolean isMasked() {
        return masked;
    }

    /**
     * @see org.teiid.adminapi.PropertyDefinition#isModifiable()
     */
    public boolean isModifiable() {
        return modifiable;
    }

    /**
     * @see org.teiid.adminapi.PropertyDefinition#isRequired()
     */
    public boolean isRequired() {
        return required;
    }

    /**
     * @param allowedValues The allowedValues to set.
     */
    public void setAllowedValues(Collection allowedValues) {
        this.allowedValues = allowedValues;
    }

    /**
     * @param defaultValue The defaultValue to set.
     */
    public void setDefaultValue(Serializable defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * @param description The description to set.
     */
    public void setDescription(String description) {
        this.description = description;
    }


    /**
     * @param displayName The displayName to set.
     */
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }


    /**
     * @param expert The value of expert to set.
     */
    public void setAdvanced(boolean expert) {
        this.advanced = expert;
    }

    /**
     * @param masked The value of masked to set.
     */
    public void setMasked(boolean masked) {
        this.masked = masked;
    }
    /**
     * @param modifiable The value of modifiable to set.
     */
    public void setModifiable(boolean modifiable) {
        this.modifiable = modifiable;
    }

    /**
     * @param propertyTypeClassName The propertyTypeName to set.
     */
    public void setPropertyTypeClassName(String propertyTypeClassName) {
        this.propertyTypeClassName = propertyTypeClassName;
    }


    /**
     * @param required The value of required to set.
     */
    public void setRequired(boolean required) {
        this.required = required;
    }

    /**
     * @param requiresRestart The value of requiresRestart to set.
     */
    public void setRequiresRestart(RestartType requiresRestart) {
        this.requiresRestart = requiresRestart;
    }

    @Override
    public boolean isConstrainedToAllowedValues() {
        return allowedValues != null && !allowedValues.isEmpty();
    }

    @Override
    public String getCategory() {
        return this.category;
    }

    public void setCategory(String category) {
        this.category = category;
    }
}
