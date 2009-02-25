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

package com.metamatrix.connector.object.extension.source;


import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.object.ObjectSourceMethodManager;
import com.metamatrix.connector.object.extension.IObjectCommand;
import com.metamatrix.connector.object.extension.IObjectSource;

/**
 * The basic implementation of the IObjectSource.
 * This 
 */
public abstract class BaseObjectSource implements IObjectSource {
    private ObjectSourceMethodManager mm =null;
    private Object api;
     
    private boolean isAlive = false;
        
    /**
     * Constructor.
     * @param env
     * @throws AdminException 
     */    
    protected BaseObjectSource(Object api) throws ConnectorException {
        this.api = api;
        this.mm = new ObjectSourceMethodManager(this.api.getClass());
        this.isAlive();
    }
    

    /** 
     * @see com.metamatrix.connector.object.extension.IObjectSource#getSource()
     * @since 4.3
     */
    public Object getSource() {
        return api;
    }

    /** 
     * @see com.metamatrix.connector.object.extension.IObjectSource#getObjects(java.lang.String, java.util.Map)
     * @since 4.3
     */
    public List getObjects(IObjectCommand command) throws ConnectorException {
        try {
            Object data = executeMethod(command);
            if (command.hasResults()) {
                return translate(data);
            }
            return null;
        } catch (ConnectorException ce) {
            throw ce;
        } catch (Exception e) {
            throw new ConnectorException(e);
        }

    }
    
    protected Object executeMethod(IObjectCommand command) throws Exception {
        Method m = getMethod(command);
        if (m != null) {        
            return m.invoke(this.api, command.getCriteriaValues().toArray());
        }
        return null;
        
    }

    /** 
     * @see com.metamatrix.connector.object.extension.ISourceTranslator#getMethod(java.lang.String, java.util.List)
     * @since 4.3
     */
    protected Method getMethod(IObjectCommand objectCommand) throws SecurityException,
                                              NoSuchMethodException {
    
        Method m = mm.getMethodFromAPI(objectCommand);
        
        if (m == null) {
            // return null, the calling class will handle throws
            // no method defined exception
            return null;
        }
        return m;
    }
    
    // translate into a List result 
    protected List translate(Object object) throws Exception {
        if (object == null) {
            return Collections.EMPTY_LIST;
        }
        List resultData = new ArrayList();
        if (object.getClass().isArray()) {
            List resultRows = Arrays.asList((Object[]) object);
            resultData = new ArrayList(resultRows.size());
            resultData.addAll(resultRows);
            
        } else if (object instanceof Collection) {
            
            Collection resultRows = (Collection) object;
            if (resultRows.isEmpty()) {
                return Collections.EMPTY_LIST;
            }

            resultData = new ArrayList(resultRows.size());
            resultData.addAll(resultRows);

        } else {
            List resultRow = new ArrayList(1);
            resultRow.add(object);
            resultData.add(resultRow);
        }             
        return resultData;            
    }      
    

    /** 
     * @see com.metamatrix.data.pool.SourceConnection#closeSource()
     * @since 4.2
     */
    public void closeSource() { 
        this.isAlive = false;
        api=null;
    }
    
    public void setIsAlive(boolean isAlive) {
        this.isAlive = isAlive;
    }
    
    
    public boolean isAlive() {
        return this.isAlive;
    }

    
}
