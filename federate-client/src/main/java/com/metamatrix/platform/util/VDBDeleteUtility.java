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

package com.metamatrix.platform.util;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;

/** 
 * @since 4.3
 */
public interface VDBDeleteUtility {

    /**
     * Deletes all VDB versions that have been marked for delete and that have no
     * user sessions logged in to them.
     * @throws MetaMatrixProcessingException If an error occurs while deleting the VDB.
     * @throws MetaMatrixComponentException If an erorr occurs while accessing components required to delete the VDB.
     */
    public abstract void deleteVDBsMarkedForDelete() throws MetaMatrixProcessingException, MetaMatrixComponentException;

    /**
     * Deletes all VDB versions that have been marked for delete and that have no
     * user sessions logged in to them.<br></br><strong>NOTE</strong>: This method
     * is only to be called from a cleanup handler that has itself been called
     * when the given session is closing.  If the given session is the only one
     * logged in to a VDB version that's been marked for delete, it will be deleted.
     * @param id The session that is about to be closed.
     * @throws MetaMatrixProcessingException If an error occurs while deleting the VDB.
     * @throws MetaMatrixComponentException If an erorr occurs while accessing components required to delete the VDB.
     */
    public abstract void deleteVDBsMarkedForDelete(MetaMatrixSessionID id) throws MetaMatrixProcessingException, MetaMatrixComponentException;

}