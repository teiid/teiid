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

package com.metamatrix.connector.sysadmin.extension.value;

import java.sql.Date;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.sysadmin.SysAdminPropertyNames;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;


/** 
 * @since 4.2
 */
public class TestExtensionValue extends TestCase {

    public void testJavaUtilDateToSqlDateValue() throws Exception {
        
        JavaUtilDateToSqlDateValueTranslator t = new JavaUtilDateToSqlDateValueTranslator();
        
        t.initialize(createEnvironment());
        
        java.util.Date jud = new java.util.Date();
        
        
        Object o = t.translate(jud, null, null);

        
        assertNotNull("Did not translate java.util.Date", o); //$NON-NLS-1$
        
        assertSame("Did not translate into java.sql.Date", Date.class, o.getClass()); //$NON-NLS-1$
    }

    private ConnectorEnvironment createEnvironment() {
        Properties properties = new Properties();
        
        properties.setProperty(SysAdminPropertyNames.SYSADMIN_CONNECTION_FACTORY_CLASS, "com.metamatrix.connector.sysadmin.FakeSysAdminConnectionFactory"); //$NON-NLS-1$
        ConnectorEnvironment environment = new ConnectorEnvironmentImpl(properties, new SysLogger(false), null);

        return environment;
    }
    
}
