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

import org.teiid.connector.metadata.runtime.Group;

/**
 * Represents a group in the language objects.  An example of a group would 
 * be a table reference in the FROM clause.  An IGroup may have a context name
 * used in references to this group. 
 */
public interface IGroup extends IMetadataReference<Group>, IFromItem, ILanguageObject {
    
    /**
     * Get the name of the group as defined in the VDB. This is null if the 
     * context is the same as the definition.
     * @return Actual group name
     */
    String getDefinition();

    /**
     * Get the aliased name this group uses for references in the command.
     * @return Context name
     */
    String getContext();
    
    /**
     * Set the name of the group as defined in the VDB. This is null if the 
     * context is the same as the definition.
     * @param definition The definition
     */
    void setDefinition(String definition);

    /**
     * Set the aliased name this group uses for references in the command.
     * @param context Context name
     */
    void setContext(String context);
    
}
