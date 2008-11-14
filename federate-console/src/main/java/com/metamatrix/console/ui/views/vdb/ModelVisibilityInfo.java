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

package com.metamatrix.console.ui.views.vdb;

/**
 * NOTE-- despite the name of ModelVisiblityInfo, this class has been expanded to include
 * the instance variables 'multipleSourceEligible' and 'hasMultipleSources, respecitively
 * indicating whether or not the model can have and does have multiple sources.  
 * BWP 01/18/05
 */
public class ModelVisibilityInfo {
    private String modelName;
    private int modelVersion;
    private String modelType;
    private boolean visible;
    private boolean multipleSourceEligible; //input information
    private boolean multipleSourceFlagEditable; //input information
    private boolean multipleSourcesSelected; //output information

    public ModelVisibilityInfo(String name, int version, String type,
            boolean vis, boolean multSourceEligible, boolean multSourceFlagEditable,
            boolean multSourcesSelected) {
        super();
        modelName = name;
        modelVersion = version;
        modelType = type;
        visible = vis;
        multipleSourceEligible = multSourceEligible;
        multipleSourceFlagEditable = multSourceFlagEditable;
        multipleSourcesSelected = multSourcesSelected;
    }

    public String getModelName() {
        return modelName;
    }

    public int getModelVersion() {
        return modelVersion;
    }

    public String getModelType() {
        return modelType;
    }

    public boolean isVisible() {
        return visible;
    }

    public void setVisible(boolean flag) {
        visible = flag;
    }
    
    public boolean isMultipleSourceEligible() {
        return multipleSourceEligible;
    }
    
    public boolean isMultipleSourceFlagEditable() {
        return multipleSourceFlagEditable;
    }
    
    public boolean isMultipleSourcesSelected() {
        return multipleSourcesSelected;
    }
}
