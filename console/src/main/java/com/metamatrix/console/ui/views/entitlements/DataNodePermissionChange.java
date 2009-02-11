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

public class DataNodePermissionChange {
    private PermissionNode node;
    private int authorizationType;
    private boolean checked;

    public DataNodePermissionChange(PermissionNode nod, int authType,
            boolean chekd) {
        super();
        node = nod;
        authorizationType = authType;
        checked = chekd;
    }

    public PermissionNode getNode() {
        return node;
    }

    public int getAuthorizationType() {
        return authorizationType;
    }

    public boolean isChecked() {
        return checked;
    }

    public boolean isOppositeOf(DataNodePermissionChange change) {
        boolean opposite = false;
        if (this.getNode() == change.getNode()) {
            if (this.getAuthorizationType() == change.getAuthorizationType()) {
                if (this.isChecked() != change.isChecked()) {
                    opposite = true;
                }
            }
        }
        return opposite;
    }

    public String toString() {
        String str = "DataNodePermissionChange: node=" + node +
                ",authorizationType=" +
                AuthorizationsModel.columnNumToString(authorizationType) +
                ",checked=" + checked;
        return str;
    }
}
