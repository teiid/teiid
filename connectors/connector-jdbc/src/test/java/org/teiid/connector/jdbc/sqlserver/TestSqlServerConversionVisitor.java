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

import java.util.List;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.jdbc.TranslationHelper;
import org.teiid.connector.language.ICommand;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.unittest.FakeMetadataStore;

/**
 */
public class TestSqlServerConversionVisitor {

    private static SqlServerSQLTranslator trans = new SqlServerSQLTranslator();
    
    @BeforeClass
    public static void setup() throws ConnectorException {
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
    }

    public String getTestVDB() {
        return TranslationHelper.PARTS_VDB;
    }

    public String getBQTVDB() {
        return TranslationHelper.BQT_VDB;
    }
    
    public void helpTestVisitor(String vdb, String input, String expectedOutput) throws ConnectorException {
    	TranslationHelper.helpTestVisitor(vdb, input, expectedOutput, trans);
    }

    @Test
    public void testModFunction() throws Exception {
        String input = "SELECT mod(CONVERT(PART_ID, INTEGER), 13) FROM parts"; //$NON-NLS-1$
        String output = "SELECT (cast(PARTS.PART_ID AS int) % 13) FROM PARTS";  //$NON-NLS-1$

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
        String output = "SELECT {fn dayofmonth(cast(PARTS.PART_ID AS datetime))} FROM PARTS"; //$NON-NLS-1$
    
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
    
    @Test public void testConvert() throws Exception {
        String input = "select convert(timestampvalue, date), convert(timestampvalue, string), convert(datevalue, string) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT cast(replace(convert(varchar, SmallA.TimestampValue, 102), '.', '-') AS datetime), convert(varchar, SmallA.TimestampValue, 21), replace(convert(varchar, SmallA.DateValue, 102), '.', '-') FROM SmallA"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            output);        
    }
    
    @Test public void testUniqueidentifier() throws Exception { 
        FakeMetadataObject ldapModel = FakeMetadataFactory.createPhysicalModel("foo"); //$NON-NLS-1$
        FakeMetadataObject table = FakeMetadataFactory.createPhysicalGroup("bar", ldapModel); //$NON-NLS-1$
        String[] elemNames = new String[] {
            "x"  //$NON-NLS-1$ 
        };
        String[] elemTypes = new String[] {  
            DataTypeManager.DefaultDataTypes.STRING
        };
        
        List cols = FakeMetadataFactory.createElements(table, elemNames, elemTypes);
        
        FakeMetadataObject obj = (FakeMetadataObject) cols.get(0);
        obj.putProperty(FakeMetadataObject.Props.NATIVE_TYPE, "uniqueidentifier"); //$NON-NLS-1$
        
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(ldapModel);
        store.addObject(table);     
        store.addObjects(cols);
        
        // Create the facade from the store
        QueryMetadataInterface metadata = new FakeMetadataFacade(store);
        
        TranslationUtility tu = new TranslationUtility(metadata);
        ICommand command = tu.parseCommand("select max(x) from bar"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("SELECT MAX(cast(bar.x as char(36))) FROM bar", trans, command); //$NON-NLS-1$
    }
       
}
