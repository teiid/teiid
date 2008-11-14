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

package com.metamatrix.data.language;

import java.util.List;

/**
 * Represents an UPDATE command in the language objects.
 */
public interface IUpdate extends ICommand {

    /**
     * Get group that is being inserted into.
     * @return Insert group
     */
    IGroup getGroup();

    /**
     * Get list of changes that should occur in the UPDATE - every
     * change is of the form "element = expression" and is stored in aa 
     * ICompareCriteria.
     * @return List of ICompareCriteria
     */
    List getChanges();

    /** 
     * Get criteria that is being used with the delete - may be null
     * @return Criteria, may be null
     */
    ICriteria getCriteria();
    
    /**
     * Set group that is being inserted into.
     * @param group Insert group
     */
    void setGroup(IGroup group);

    /**
     * Set list of changes that should occur in the UPDATE - every
     * change is of the form "element = expression" and is stored in aa 
     * ICompareCriteria.
     * @param changes List of ICompareCriteria
     */
    void setChanges(List changes);

    /** 
     * Set criteria that is being used with the delete - may be null
     * @param criteria Criteria, may be null
     */
    void setCriteria(ICriteria criteria);
    
}
