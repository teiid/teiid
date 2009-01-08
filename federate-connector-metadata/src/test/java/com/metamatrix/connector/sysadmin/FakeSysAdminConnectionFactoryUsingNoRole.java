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

import com.metamatrix.connector.sysadmin.extension.ISourceTranslator;
import com.metamatrix.connector.sysadmin.extension.ISysAdminConnectionFactory;
import com.metamatrix.connector.sysadmin.extension.ISysAdminSource;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;

/**
 */
public class FakeSysAdminConnectionFactoryUsingNoRole implements ISysAdminConnectionFactory {
    
    Object api = null;
    ConnectorEnvironment env;

    /** 
     * @see com.metamatrix.connector.sysadmin.extension.ISysAdminConnectionFactory#init(com.metamatrix.data.api.ConnectorEnvironment)
     * @since 4.3
     */
    public void init(ConnectorEnvironment environment) throws ConnectorException {
        this.env = environment;
    }
    
    
    /** 
     * @see com.metamatrix.connector.sysadmin.extension.ISysAdminConnectionFactory#getObjectSource(com.metamatrix.data.api.SecurityContext)
     * @since 4.3
     */
    public ISysAdminSource getObjectSource(SecurityContext context) throws ConnectorException {
      
            try {       
                Object o = FakeObjectUtil.getInstanceBasedOnNoRole(context.getUser(), context.getConnectionIdentifier());
                Class clzz = o.getClass();
                                
                ISourceTranslator sourceTranslator = new FakeObjectSourceTranslator(clzz);
                sourceTranslator.initialize(env);
                
                 return new SysAdminObjectSource(o, env, sourceTranslator);
                         
            } catch (Exception err) {
                throw new ConnectorException(err);                        
               
            }            

    }





}
