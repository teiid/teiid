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

import java.util.Date;
import java.util.Properties;

import com.metamatrix.metadata.runtime.api.MetaBaseInfo;

public class BasicMetaBaseInfo implements MetaBaseInfo{
    String version;
    Date versionDate;
    Properties properties;
    long uid;

    public BasicMetaBaseInfo(String version, Date date) {
        this.version = version;
        this.versionDate = date;
    }

    public String getVersion(){
        return this.version;
    }

    public Date getVersionDate(){
        return this.versionDate;
    }

    public Properties getProperties(){
        return this.properties;
    }

    public long getUID(){
        return this.uid;
    }

    public void setUID(long uid){
        this.uid = uid;
    }

    public void setProperties(Properties properties){
        this.properties = properties;
    }
} 
