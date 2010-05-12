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

package org.teiid.translator.jdbc.derby;

import org.teiid.translator.ConnectorCapabilities;
import org.teiid.translator.ConnectorException;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.jdbc.EscapeSyntaxModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.db2.DB2SQLTranslator;
import org.teiid.translator.jdbc.oracle.LeftOrRightFunctionModifier;



/** 
 * @since 4.3
 */
public class DerbySQLTranslator extends DB2SQLTranslator {
	
	private String version = DerbyCapabilities.TEN_1;
	
	@Override
	public void initialize(JDBCExecutionFactory env) throws ConnectorException {
		super.initialize(env);
		//additional derby functions
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new EscapeSyntaxModifier()); 
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new EscapeSyntaxModifier()); 
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        
        //overrides of db2 functions
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new EscapeSyntaxModifier()); 
    }  
 
    @Override
    public boolean addSourceComment() {
        return false;
    }
    
    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return DerbyCapabilities.class;
    }
    
    @Override
    public boolean supportsExplicitNullOrdering() {
    	return version.compareTo(DerbyCapabilities.TEN_4) >= 0;
    }
    
    public void setDatabaseVersion(String version) {
    	this.version = version;
    }

}
