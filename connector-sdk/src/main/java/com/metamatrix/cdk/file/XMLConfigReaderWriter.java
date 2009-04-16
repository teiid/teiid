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

package com.metamatrix.cdk.file;

import java.io.IOException;
import java.io.InputStream;

import com.metamatrix.common.config.api.*;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.util.*;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;

public class XMLConfigReaderWriter implements ConfigReaderWriter {

    private ConfigurationObjectEditor editor;
    private ConfigurationImportExportUtility utility;
        
    
    public ComponentType loadConnectorType(InputStream stream) throws InvalidConfigurationElementException, IOException {             
		return getImportExportUtility().importComponentType(stream, getEditor(), null);
    }
    
	public Object[] loadConnectorBinding(InputStream stream) throws ConfigObjectsNotResolvableException, InvalidConfigurationElementException, IOException {             
		String[] name =  {null, null};
        
		return getImportExportUtility().importConnectorBindingAndType(stream, getEditor(), name);
	}
    
    private ConfigurationImportExportUtility getImportExportUtility() {
        if (utility == null) {
            utility =  new XMLConfigurationImportExportUtility();
        }
        
        return utility;
    }
    
    private ConfigurationObjectEditor getEditor() {
        if (editor == null) {
            editor = new BasicConfigurationObjectEditor();
        }
        return editor;
    }
	    
}
