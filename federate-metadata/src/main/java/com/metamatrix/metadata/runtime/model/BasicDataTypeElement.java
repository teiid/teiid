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

package com.metamatrix.metadata.runtime.model;

import com.metamatrix.metadata.runtime.api.DataType;
import com.metamatrix.metadata.runtime.api.DataTypeElement;
import com.metamatrix.modeler.core.metadata.runtime.MetadataConstants;

public class BasicDataTypeElement extends BasicMetadataObject implements DataTypeElement{

    private boolean excludeData;
    private int scale;
    private int length;
    private boolean isNullable;
    private DataType dataType;
    private long dtUID = MetadataConstants.NOT_DEFINED_LONG;
    private int position;

    public BasicDataTypeElement(BasicDataTypeElementID dataTypeElementID, BasicVirtualDatabaseID virtualDBID) {
        super(dataTypeElementID, virtualDBID);
    }

    public boolean excludeData(){
        return this.excludeData;
    }

    public int getScale(){
        return this.scale;
    }

    public int getLength(){
        return this.length;
    }

    public boolean isNullable(){
        return this.isNullable;
    }

    public void setExcludeData(boolean ed){
        this.excludeData = ed;
    }

    public void setScale(int scale){
        this.scale = scale;
    }

    public void setLength(int length){
        this.length = length;
    }

    public void setIsNullable(boolean isNullable){
        this.isNullable = isNullable;
    }

    public DataType getDataType(){
        if(dataType.getRuntimeType() != null){
	      return dataType.getRuntimeType();
    	}
    	return dataType;
    }
    
    public DataType getActualDataType(){
    	return dataType;
    }

    public void setDataType(DataType dt){
        this.dataType = dt;
    }

    public long getDataTypeUID(){
        return this.dtUID;
    }

    public void setDataTypeUID(long dtUID){
        this.dtUID = dtUID;
    }
    public void setPosition(int position){
        this.position = position;
    }
    public int getPosition(){
        return this.position;
    }
}