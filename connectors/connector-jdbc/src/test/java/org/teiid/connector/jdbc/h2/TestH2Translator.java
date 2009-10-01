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

package org.teiid.connector.jdbc.h2;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.jdbc.MetadataFactory;

import com.metamatrix.cdk.api.EnvironmentUtility;

public class TestH2Translator {
	
    private static H2Translator TRANSLATOR; 

    @BeforeClass
    public static void setUp() throws ConnectorException {
        TRANSLATOR = new H2Translator();        
        TRANSLATOR.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
    }
	
	@Test public void testTimestampDiff() throws Exception {
		String input = "select timestampdiff(SQL_TSI_FRAC_SECOND, timestampvalue, {d'1970-01-01'}) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT datediff('MILLISECOND', SmallA.TimestampValue, TIMESTAMP '1970-01-01 00:00:00.0') * 1000000 FROM SmallA";  //$NON-NLS-1$
        
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB, input, output, TRANSLATOR);
	}
	
	@Test public void testTimestampAdd() throws Exception {
		String input = "select timestampadd(SQL_TSI_FRAC_SECOND, 2, datevalue) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT cast(dateadd('MILLISECOND', (2 / 1000000), cast(SmallA.DateValue AS timestamp)) AS date) FROM SmallA";  //$NON-NLS-1$
        
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB, input, output, TRANSLATOR);
	}
	
	@Test public void testTimestampAdd1() throws Exception {
		String input = "select timestampadd(SQL_TSI_HOUR, intnum, {t'00:00:00'}) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT cast(dateadd('HOUR', SmallA.IntNum, TIMESTAMP '1970-01-01 00:00:00.0') AS time) FROM SmallA";  //$NON-NLS-1$
        
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB, input, output, TRANSLATOR);
	}


}
