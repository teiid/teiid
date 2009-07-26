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

package org.teiid.connector.jdbc.derby;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.jdbc.db2.DB2SQLTranslator;
import org.teiid.connector.jdbc.oracle.LeftOrRightFunctionModifier;
import org.teiid.connector.jdbc.translator.EscapeSyntaxModifier;



/** 
 * @since 4.3
 */
public class DerbySQLTranslator extends DB2SQLTranslator {
	
	public static final String DATABASE_VERSION = "DatabaseVersion"; //$NON-NLS-1$

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);
		//additional derby functions
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new EscapeSyntaxModifier()); 
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new EscapeSyntaxModifier()); 
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        
        //overrides of db2 functions
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new EscapeSyntaxModifier()); 
        registerFunctionModifier(SourceSystemFunctions.CONVERT, new DerbyConvertModifier(getLanguageFactory())); 
    }  
 
    @Override
    public boolean addSourceComment() {
        return false;
    }
    
    @Override
    public String getDefaultConnectionTestQuery() {
    	return "Select 0 from sys.systables where 1 = 2"; //$NON-NLS-1$
    }
    
    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return DerbyCapabilities.class;
    }
    
    @Override
    public ConnectorCapabilities getConnectorCapabilities()
    		throws ConnectorException {
    	ConnectorCapabilities capabilities = super.getConnectorCapabilities();
    	if (capabilities instanceof DerbyCapabilities) {
    		((DerbyCapabilities)capabilities).setVersion(getEnvironment().getProperties().getProperty(DATABASE_VERSION, DerbyCapabilities.TEN_1));
    	}
    	return capabilities;
    }

}
