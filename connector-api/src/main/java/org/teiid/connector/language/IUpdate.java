/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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

package org.teiid.connector.language;


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
     * change is of the form "element = expression".
     * @return ISetClauseList
     */
    ISetClauseList getChanges();

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
     * change is of the form "element = expression".
     * @param changes
     */
    void setChanges(ISetClauseList changes);

    /** 
     * Set criteria that is being used with the delete - may be null
     * @param criteria Criteria, may be null
     */
    void setCriteria(ICriteria criteria);
    
}
