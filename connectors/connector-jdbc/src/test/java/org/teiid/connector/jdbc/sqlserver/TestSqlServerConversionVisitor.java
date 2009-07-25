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

package org.teiid.connector.jdbc.sqlserver;

import java.util.Properties;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.jdbc.MetadataFactory;
import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.language.ICommand;

import com.metamatrix.cdk.api.EnvironmentUtility;

/**
 */
public class TestSqlServerConversionVisitor {

    private static SqlServerSQLTranslator trans = new SqlServerSQLTranslator();
    
    @BeforeClass
    public static void setup() throws ConnectorException {
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
    }

    public String getTestVDB() {
        return MetadataFactory.PARTS_VDB;
    }

    public String getBQTVDB() {
        return MetadataFactory.BQT_VDB;
    }
    
    public void helpTestVisitor(String vdb, String input, String expectedOutput) throws ConnectorException {
		helpTestVisitor(vdb, input, new String[] {expectedOutput});
    }
    
    public void helpTestVisitor(String vdb, String input, String[] expectedOutputs) throws ConnectorException {
        ICommand obj = MetadataFactory.helpTranslate(vdb, input);
        TranslatedCommand tc = new TranslatedCommand(EnvironmentUtility.createSecurityContext("user"), trans); //$NON-NLS-1$
        tc.translateCommand(obj);
        Assert.assertEquals("Did not get correct sql", expectedOutputs[0], tc.getSql());             //$NON-NLS-1$
    }

    @Test
    public void testModFunction() throws Exception {
        String input = "SELECT mod(CONVERT(PART_ID, INTEGER), 13) FROM parts"; //$NON-NLS-1$
        String output = "SELECT (convert(int, PARTS.PART_ID) % 13) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            output);
    } 

    @Test
    public void testConcatFunction() throws Exception {
        String input = "SELECT concat(part_name, 'b') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT (PARTS.PART_NAME + 'b') FROM PARTS"; //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }    

    @Test
    public void testDayOfMonthFunction() throws Exception {
        String input = "SELECT dayofmonth(convert(PARTS.PART_ID, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT day(convert(datetime, PARTS.PART_ID)) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }

    @Test
    public void testRowLimit() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 * FROM (SELECT SmallA.IntKey FROM SmallA) AS X"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            output);        
    }
    
    @Test
    public void testUnionLimitWithOrderBy() throws Exception {
        String input = "select intkey from bqt1.smalla union select intnum from bqt1.smalla order by intkey limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 * FROM (SELECT SmallA.IntKey FROM SmallA UNION SELECT SmallA.IntNum FROM SmallA) AS X ORDER BY intkey"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            output);        
    }
    
    @Test
    public void testDateFunctions() throws Exception {
        String input = "select dayName(timestampValue), dayOfWeek(timestampValue), quarter(timestampValue) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT {fn dayName(SmallA.TimestampValue)}, {fn dayOfWeek(SmallA.TimestampValue)}, {fn quarter(SmallA.TimestampValue)} FROM SmallA"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            output);        
    }
       
}
