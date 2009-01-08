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

import com.metamatrix.admin.api.objects.AdminObject;
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
    
     private ConnectorEnvironment env;
     private SysAdminSourceTranslator sourceTranslator;
     
    /**
     *
     */
    public SysAdminConnectionFactory() { 
        super();

    }
    
    public void init(final ConnectorEnvironment environment) throws ConnectorException {
        this.env = environment;
        
        sourceTranslator = new SysAdminSourceTranslator(AdminObject.class);
        sourceTranslator.initialize(env);
    }
    
    
    /** 
     * @see com.metamatrix.connector.object.extension.source.BaseSourceConnectionFactory#getObjectSource(com.metamatrix.data.pool.ConnectorIdentity)
     * @since 4.3
     */
    public ISysAdminSource getObjectSource(final SecurityContext context) throws ConnectorException {
		 ClientServiceRegistry registry = (ClientServiceRegistry)((ConnectorEnvironmentImpl)env).findResource(DQPServiceNames.REGISTRY_SERVICE);
		 ServerAdmin serverAdmin = registry.getClientService(ServerAdmin.class);
		 return new SysAdminObjectSource(serverAdmin, env, sourceTranslator);
    }

}
