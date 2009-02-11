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

package com.metamatrix.console.ui.views.entitlements;

import com.metamatrix.platform.admin.api.PermissionNode;

public class DataNodePermissionsWithNodeName {
    private PermissionNode correspondingNode;
    private DataNodePermissions oldPermissions;
    private DataNodePermissions newPermissions;
    private String nodeDisplayName;
    private String nodeFullName;

    public DataNodePermissionsWithNodeName(PermissionNode node,
            DataNodePermissions oldPerm, DataNodePermissions newPerm,
            String displayName, String fullName) {
        super();
        correspondingNode = node;
        oldPermissions = oldPerm;
        newPermissions = newPerm;
        nodeDisplayName = displayName;
        nodeFullName = fullName;
    }

    public PermissionNode getCorrespondingNode() {
        return correspondingNode;
    }

    public DataNodePermissions getOldPermissions() {
        return oldPermissions;
    }

    public DataNodePermissions getNewPermissions() {
        return newPermissions;
    }

    public String getNodeDisplayName() {
        return nodeDisplayName;
    }

    public String getNodeFullName() {
        return nodeFullName;
    }
    
    public String toString() {
    	String str = "DataNodePermissionsWithNodeName: nodeDisplayName=" +
    			getNodeDisplayName() + ", nodeFullName= " + getNodeFullName() +
    			", oldPermissions=" + getOldPermissions().toPermissionsString()
    			+ ", newPermissions=" +
    			getNewPermissions().toPermissionsString();
    	return str;
    }
}
