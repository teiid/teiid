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

package com.metamatrix.connector.sysadmin;


import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.connector.sysadmin.extension.IObjectCommand;
import com.metamatrix.connector.sysadmin.extension.ISourceTranslator;
import com.metamatrix.connector.sysadmin.extension.ISysAdminSource;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;

/**
 * The basic implementation of the IObjectSource.
 * This 
 */
public class SysAdminObjectSource implements ISysAdminSource  {

    private SysAdminMethodManager mm =null;
    private Object api;
    private ISourceTranslator sourceTranslator = null;
    
     
    private boolean isAlive = false;
    private boolean isFailed = false;
        
    /**
     * Constructor.
     * @param env
     * @throws AdminException 
     */    
    protected SysAdminObjectSource(Object sourceConnection, Class apiClass, ConnectorEnvironment environment, ISourceTranslator sourceTranslator) throws ConnectorException {
        this.api = sourceConnection;
        this.mm = new SysAdminMethodManager(sourceConnection.getClass());
        this.sourceTranslator = sourceTranslator;
       
        this.isAlive();
    }
    

    /** 
     * @see com.metamatrix.connector.object.extension.IObjectSource#getSource()
     * @since 4.3
     */
    public Object getSource() {
        return api;
    }
    
    
    public ISourceTranslator getSourceTranslator() {
        return this.sourceTranslator;
    }

    /** 
     * @see com.metamatrix.connector.object.extension.IObjectSource#getObjects(java.lang.String, java.util.Map)
     * @since 4.3
     */
    public List getObjects(IObjectCommand command) throws ConnectorException {
        try {
            Object data = executeMethod(command);
            if (command.hasResults()) {
                return convert(data);
            }
            return null;
        } catch (ConnectorException ce) {
            isFailed = true;
            throw ce;
        } catch (Throwable e) {
            isFailed = true;
            throw new ConnectorException(e);
        }

    }
    
    private Object executeMethod(IObjectCommand command) throws Throwable {
        Method m = getMethod(command);
        if (m != null) {        
           try {
               return m.invoke(this.api, command.getCriteriaValues().toArray());
           } catch (java.lang.reflect.InvocationTargetException ivt) {
               if (ivt.getCause() != null) {
                   throw ivt.getCause();
               }
               throw ivt;
           }
        } 
       
        
        return null;
        
    }

    /** 
     * @see com.metamatrix.connector.object.extension.ISourceTranslator#getMethod(java.lang.String, java.util.List)
     * @since 4.3
     */
    private Method getMethod(IObjectCommand objectCommand) throws SecurityException,
                                              NoSuchMethodException {
    
        Method m = mm.getMethodFromAPI(objectCommand);
        
        if (m == null) {
            // return null, the calling class will handle throws
            // no method defined exception
            return null;
        }
        return m;
    }
    
    // convert into a List result 
    private List convert(Object object) {

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
        this.api=null;
        this.sourceTranslator = null;
        this.mm = null;
    }
    
    public void setIsAlive(boolean isAlive) {
        this.isAlive = isAlive;
    }
    
    
    public boolean isAlive() {
        return this.isAlive;
    }


    /** 
     * @see com.metamatrix.connector.object.extension.IObjectSource#isFailed()
     * @since 4.3
     */
    public boolean isFailed() {
        return this.isFailed;
    }
    
}
