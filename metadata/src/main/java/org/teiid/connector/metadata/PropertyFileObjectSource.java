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

package org.teiid.connector.metadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * Read a property file and deliver the results as an object source. 
 * This plugs into the object connector to make a property file visible as a relational table.
 */
public class PropertyFileObjectSource implements IObjectSource {
    private String propertyFilePath;
    
    public PropertyFileObjectSource() {
    	this("org/teiid/connector/metadata/enum/"); //$NON-NLS-1$
    }
    
    public PropertyFileObjectSource(String propertyFilePath) {
        this.propertyFilePath = propertyFilePath;
    }
    
    /* 
     * @see com.metamatrix.connector.metadata.internal.IObjectSource#getObjects(java.lang.String, java.util.Map)
     */
    public Collection getObjects(String propertyFileName, Map criteria) {
        if (criteria != null && criteria.size() >0) {
            throw new UnsupportedOperationException("Criteria is not supported"); //$NON-NLS-1$
        }
        InputStream input = null;
        try {
        	propertyFileName = expandPropertyFileName(propertyFileName);
        	
            input = this.getClass().getClassLoader().getResourceAsStream(propertyFileName);
            if (input == null) {
            	throw new MetaMatrixRuntimeException(propertyFileName+" file not found");
            }
            //input = new BufferedInputStream(new FileInputStream(propertyFileName));
            Properties properties = new Properties();
            properties.load(input);
            List results = new ArrayList();
            for (Iterator iterator=properties.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry entry = (Map.Entry) iterator.next();
                PropertyHolder holder = new PropertyHolder(new Integer((String) entry.getKey()));
                holder.setValue(entry.getValue());
                results.add(holder);
            }
            return results;
        } catch (FileNotFoundException e) {
            throw new MetaMatrixRuntimeException(e);
        } catch (IOException e) {
            throw new MetaMatrixRuntimeException(e);
        } finally {
            if (input != null) {
                try {
					input.close();
				} catch (IOException e) {
				}
            }
        }
    }

    private String expandPropertyFileName(String propertyFileName) {
        if (propertyFilePath == null) {
            return propertyFileName;    
        }
        return propertyFilePath + propertyFileName;
    }

}
