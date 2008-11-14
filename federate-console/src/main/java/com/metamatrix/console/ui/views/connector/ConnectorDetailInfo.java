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

package com.metamatrix.console.ui.views.connector;

import java.util.Date;

public class ConnectorDetailInfo {
    private String name;
    private String description;
    private String connection;
    private Date created;
    private String createdBy;
    private Date registered;
    private String registeredBy;
    
    public ConnectorDetailInfo(String nam, String con, Date cre, String creBy,
                               Date reg, String regBy) {
                           super();
                           name = nam;
                           connection = con;
                           created = cre;
                           createdBy = creBy;
                           registered = reg;
                           registeredBy = regBy;
                       }    

    public ConnectorDetailInfo(String nam, String desc, String con, Date cre, String creBy,
            Date reg, String regBy) {
        super();
        name = nam;
        description = desc;
        connection = con;
        created = cre;
        createdBy = creBy;
        registered = reg;
        registeredBy = regBy;
    }

    public String getName() {
        return name;
    }
    
    public String getDescription() {
        return description;
    }

    public String getConnection() {
        return connection;
    }

    public Date getCreated() {
        return created;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public Date getRegistered() {
        return registered;
    }

    public String getRegisteredBy() {
        return registeredBy;
    }
}
