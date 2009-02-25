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

package com.metamatrix.connector.object.extension.command;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IMetadataReference;
import org.teiid.connector.metadata.runtime.MetadataObject;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.object.extension.IObjectCommand;
import com.metamatrix.connector.object.util.ObjectConnectorUtil;
import com.metamatrix.core.util.ArgCheck;


/** 
 * @since 4.3
 */
public abstract class ObjectCommand implements IObjectCommand {
    
    private final RuntimeMetadata metadata;
    private final ICommand command;
        
    
    public ObjectCommand(final RuntimeMetadata metadata, final ICommand command) throws ConnectorException {
        ArgCheck.isNotNull(metadata);
        ArgCheck.isNotNull(command);

        this.metadata = metadata;
        this.command = command;
                
    }
    
    public ICommand getCommand() {
        return this.command;
    }
        
        
    public boolean hasResults() {
        return false;
    }
    
    public RuntimeMetadata getMetadata() {
        return this.metadata;
    }
    
    /**
     * A helper method used to get the nameInSource of the given object. 
     * @since 4.2
     */
    protected String getMetadataObjectNameInSource(IMetadataReference reference) throws ConnectorException {
        if(reference == null) {
            return null;
        }
        
        return ObjectConnectorUtil.getMetadataObjectNameInSource(getMetadata(), getCommand(), reference);
    }
    
    protected String determineName(IMetadataReference reference) throws ConnectorException {        
        String nis = getMetadataObjectNameInSource(reference);
        if (nis == null || nis.length() == 0) {
            MetadataObject id = reference.getMetadataObject();
            return id.getName();
        }
        return nis;
    }    
    
    
}
