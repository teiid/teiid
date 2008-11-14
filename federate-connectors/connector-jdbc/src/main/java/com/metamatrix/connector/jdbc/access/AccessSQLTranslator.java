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
package com.metamatrix.connector.jdbc.access;

import java.util.Properties;

import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.jdbc.extension.impl.BasicSQLTranslator;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ILanguageFactory;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

public class AccessSQLTranslator extends BasicSQLTranslator {
    private Properties connectorProperties;
    private ILanguageFactory languageFactory;
    
    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getTranslationVisitor()
     */
    public SQLConversionVisitor getTranslationVisitor() {
        SQLConversionVisitor visitor = new AccessSQLConversionVisitor();
        visitor.setRuntimeMetadata(getRuntimeMetadata());
        visitor.setFunctionModifiers(getFunctionModifiers());
        visitor.setProperties(connectorProperties);
        visitor.setLanguageFactory(languageFactory);
        visitor.setDatabaseTimeZone(getDatabaseTimeZone());
        return visitor;
    }
    
    /* 
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#initialize(com.metamatrix.data.api.ConnectorEnvironment, com.metamatrix.data.metadata.runtime.RuntimeMetadata)
     */
    public void initialize(ConnectorEnvironment env, RuntimeMetadata metadata) throws ConnectorException {
        super.initialize(env, metadata);
        connectorProperties = getConnectorEnvironment().getProperties();
        languageFactory = getConnectorEnvironment().getLanguageFactory();
    }
}
