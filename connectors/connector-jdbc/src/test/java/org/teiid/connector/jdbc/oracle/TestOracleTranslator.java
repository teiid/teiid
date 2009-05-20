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

package org.teiid.connector.jdbc.oracle;

import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.jdbc.MetadataFactory;
import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.ICommand;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.unittest.FakeTranslationFactory;

public class TestOracleTranslator extends TestCase {
	
	private static Translator TRANSLATOR; 

    static {
        try {
            TRANSLATOR = new OracleSQLTranslator();        
            TRANSLATOR.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
        } catch(ConnectorException e) {
            e.printStackTrace();    
        }
    }

	public void helpTestVisitor(String input, String expectedOutput) throws ConnectorException {
        // Convert from sql to objects
        ICommand obj = FakeTranslationFactory.getInstance().getAutoIncrementTranslationUtility().parseCommand(input);
        
        TranslatedCommand tc = new TranslatedCommand(EnvironmentUtility.createSecurityContext("user"), TRANSLATOR);
        tc.translateCommand(obj);
        
        // Check stuff
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }
	
	public void testInsertWithSequnce() throws Exception {
		helpTestVisitor("insert into test.group (e0) values (1)", "INSERT INTO group (e0, e1) VALUES (1, MYSEQUENCE.nextVal)");
	}
	
	public void testInsertWithSequnce1() throws Exception {
		helpTestVisitor("insert into test.group (e0, e1) values (1, 'x')", "INSERT INTO group (e0, e1) VALUES (1, 'x')");
	}
	
    public void testJoins() throws Exception {
        String input = "select smalla.intkey from bqt1.smalla inner join bqt1.smallb on smalla.stringkey=smallb.stringkey cross join bqt1.mediuma"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA INNER JOIN SmallB ON SmallA.StringKey = SmallB.StringKey CROSS JOIN MediumA"; //$NON-NLS-1$
          
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
            input, 
            output, TRANSLATOR);        
    }
    
    public void testJoins2() throws Exception {
        String input = "select smalla.intkey from bqt1.smalla cross join (bqt1.smallb cross join bqt1.mediuma)"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA CROSS JOIN (SmallB CROSS JOIN MediumA)"; //$NON-NLS-1$
      
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
            input, 
            output, TRANSLATOR);        
    }
}
