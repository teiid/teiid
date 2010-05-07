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

package org.teiid.connector.jdbc.mysql;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.connector.jdbc.TranslationHelper;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.adapter.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.mysql.MySQL5Translator;

/**
 */
public class TestMySQL5Translator {

    private static MySQL5Translator TRANSLATOR; 
    
    @BeforeClass public static void oneTimeSetup() throws ConnectorException {
        TRANSLATOR = new MySQL5Translator();        
        TRANSLATOR.initialize(new JDBCExecutionFactory());
    }

    @Test public void testChar() throws Exception {
    	String input = "SELECT intkey, CHR(CONVERT(bigintegervalue, integer)) FROM BQT1.MediumA"; //$NON-NLS-1$
        String output = "SELECT MediumA.IntKey, char(cast(MediumA.BigIntegerValue AS signed) USING ASCII) FROM MediumA"; //$NON-NLS-1$
          
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input, 
            output, TRANSLATOR);
    }
    
}
