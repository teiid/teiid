/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.console.ui.views.properties;

public class PropertyFilter {


    private boolean allProperties;//= false;
    private boolean isBasic = true;
    private boolean isExpert ;//= false;
    private boolean isModifiable;
    private boolean isReadOnly; //= false;
    private boolean isBothBE;
    private boolean isBothMR = true;
    private boolean isMRBEnabled = true;
    private String groupName;

    public PropertyFilter() {
    }

    public void setGroupName(String groupName){
        this.groupName = groupName;
    }

    public String getGroupName(){
        return this.groupName;
    }

    public void setAllProperties(boolean allProperties){
        this.allProperties = allProperties;
    }

    public boolean isAllProperties(){
        return allProperties;
    }

    public void setBasicProperties(boolean isBasic){
        this.isBasic = isBasic;
        this.isExpert = !isBasic;
        this.isBothBE = !isBasic;
    }

    public boolean isBasicProperties(){
        return isBasic;
    }

    public void setExpertProperties(boolean isExpert){
        this.isExpert = isExpert;
        this.isBasic = !isExpert;
        this.isBothBE = !isExpert;
    }

    public boolean isExpertProperties(){
        return isExpert;
    }

    public void setBothBEProperties(boolean isBothBE){
        this.isBothBE = isBothBE;
        this.isBasic = !isBothBE;
        this.isExpert = !isBothBE;
    }

    public boolean isBothBEProperties(){
        return isBothBE;
    }

    public void setModifiableProperties(boolean isModifiable){
        this.isModifiable = isModifiable;
        this.isReadOnly = !isModifiable;
        this.isBothMR = !isModifiable;
    }

    public boolean isModifiableProperties(){
        return isModifiable;
    }

    public void setReadOnlyProperties(boolean isReadOnly){
        this.isReadOnly = isReadOnly;
        this.isModifiable = !isReadOnly;
        this.isBothMR = !isReadOnly;
    }

    public boolean isReadOnlyProperties(){
        return isReadOnly;
    }

    public void setBothMRProperties(boolean isBothMR){
        this.isBothMR = isBothMR;
        this.isModifiable = !isBothMR;
        this.isReadOnly = !isBothMR;
    }

    public boolean isBothMRProperties(){
        return isBothMR;
    }

    public boolean isMRBEnable(){
        return isMRBEnabled;
    }

    public void  setIsMRBEnabled(boolean status){
        isMRBEnabled = status;
    }

    public String toString(){
        StringBuffer result = new StringBuffer();
        result.append("Content of filter "+" for " + groupName + " :\n");
        result.append("allProperties=" + allProperties + "\n");
        result.append("isBasic=" + isBasic + "\n");
        result.append("isExpert=" + isExpert + "\n");
        result.append("isModifiable=" + isModifiable + "\n");
        result.append("isReadOnly=" + isReadOnly + "\n");
        result.append("isBothBE=" + isBothBE + "\n");
        result.append("isBothMR=" + isBothMR + "\n");
        return result.toString();
    }

}
