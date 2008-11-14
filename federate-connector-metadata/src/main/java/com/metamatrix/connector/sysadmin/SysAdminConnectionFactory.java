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

/*
 */
package com.metamatrix.connector.sysadmin;



import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.connector.sysadmin.extension.ISysAdminConnectionFactory;
import com.metamatrix.connector.sysadmin.extension.ISysAdminSource;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;
import com.metamatrix.dqp.service.DQPServiceNames;

/**
 */
public class SysAdminConnectionFactory implements ISysAdminConnectionFactory {
    
    // this is the api class for which the MethodManager and the SourceTranslator uses to 
    // narrow down what methods are exposed via reflections.
     private static final String ADMIN_CLASS_NAME="com.metamatrix.admin.api.objects.AdminObject";//$NON-NLS-1$

     private ConnectorEnvironment env = null;
     private SysAdminSourceTranslator sourceTranslator = null;
     
     private Class clazz = null;
     
    /**
     *
     */
    public SysAdminConnectionFactory() { 
        super();

    }
    
    public void init(final ConnectorEnvironment environment) throws ConnectorException {
        this.env = environment;
        
        try {
            clazz = Class.forName(ADMIN_CLASS_NAME);
        } catch (ClassNotFoundException err) {
            ConnectorException e = new ConnectorException(err.getMessage());
            e.setStackTrace(err.getStackTrace());
            throw e;
        }   
        
        sourceTranslator = new SysAdminSourceTranslator(clazz);
        sourceTranslator.initialize(env);
    }
    
    
    /** 
     * @see com.metamatrix.connector.object.extension.source.BaseSourceConnectionFactory#getObjectSource(com.metamatrix.data.pool.ConnectorIdentity)
     * @since 4.3
     */
    public ISysAdminSource getObjectSource(final SecurityContext context) throws ConnectorException {
        try {

             ClientServiceRegistry registry = (ClientServiceRegistry)((ConnectorEnvironmentImpl)env).findResource(DQPServiceNames.REGISTRY_SERVICE);
             ServerAdmin serverAdmin = registry.getClientService(ServerAdmin.class);
             return new SysAdminObjectSource(serverAdmin, clazz, env, sourceTranslator);
                     
        } catch (Exception me) {
            throw new ConnectorException(me, SysAdminPlugin.Util.getString("SysAdminConnectionFactory.Unable_to_connect_to_adminapi"));  //$NON-NLS-1$                     
        }
    }

}
