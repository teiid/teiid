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

package com.metamatrix.console.ui.views.users;

import java.awt.Component;

/**
 * Interface to be implemented by a class needing notification when
 * the selection of a group, user, or role made at the left of the Users tab
 * has changed.
 */
public interface NotifyOnSelectionChange {
    /**
     * Receive notification that selection in groups tree, users tree, or
     * roles list has changed.
     *
     * @param caller        Will be the groups tree, the users tree, or the roles list
     * @param selectedItem  The new selected item (single selection is assumed).  For either tree, this is the selected node itself.  For the roles list, this is the name of the selected role.
     */
    void selectionChanged(Component caller, Object selectedItem);
}
