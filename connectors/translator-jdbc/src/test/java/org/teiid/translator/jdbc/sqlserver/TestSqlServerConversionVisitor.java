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

package org.teiid.translator.jdbc.sqlserver;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.cdk.CommandBuilder;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Command;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;


/**
 */
public class TestSqlServerConversionVisitor {

    private static SQLServerExecutionFactory trans = new SQLServerExecutionFactory();
    
    @BeforeClass
    public static void oneTimeSetup() throws TranslatorException {
        trans.start();
    }
    
    public void setUp() throws Exception {
    	trans.setDatabaseVersion(SQLServerExecutionFactory.V_2005);
    }

    public String getTestVDB() {
        return TranslationHelper.PARTS_VDB;
    }

    public String getBQTVDB() {
        return TranslationHelper.BQT_VDB;
    }
    
    public void helpTestVisitor(String vdb, String input, String expectedOutput) throws TranslatorException {
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
        String output = "SELECT TOP 100 SmallA.IntKey FROM SmallA"; //$NON-NLS-1$
               
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
    
    @Test public void testLimitWithOrderByUnrelated() throws Exception {
        String input = "select intkey from bqt1.smalla order by intnum limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 SmallA.IntKey FROM SmallA ORDER BY SmallA.IntNum"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            output);        
    }
    
    @Test
    public void testDateFunctions() throws Exception {
        String input = "select dayName(timestampValue), dayOfWeek(timestampValue), quarter(timestampValue) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT {fn dayname(SmallA.TimestampValue)}, {fn dayofweek(SmallA.TimestampValue)}, {fn quarter(SmallA.TimestampValue)} FROM SmallA"; //$NON-NLS-1$
               
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
    
    @Test public void testConvertDate() throws Exception {
        String input = "select stringkey from bqt1.smalla where BQT1.SmallA.DateValue IN (convert('2000-01-12', date), convert('2000-02-02', date))"; //$NON-NLS-1$
        String output = "SELECT SmallA.StringKey FROM SmallA WHERE SmallA.DateValue IN (CAST('2000-01-12 00:00:00.0' AS DATETIME), CAST('2000-02-02 00:00:00.0' AS DATETIME))"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            output);        
    }
    
    @Test public void testConvertDate2008() throws Exception {
    	trans.setDatabaseVersion(SQLServerExecutionFactory.V_2008);
        String input = "select stringkey from bqt1.smalla where BQT1.SmallA.DateValue IN (convert('2000-01-12', date), convert('2000-02-02', date))"; //$NON-NLS-1$
        String output = "SELECT SmallA.StringKey FROM SmallA WHERE SmallA.DateValue IN (CAST('2000-01-12' AS DATE), CAST('2000-02-02' AS DATE))"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            output);        
    }
    
    @Test public void testUniqueidentifier() throws Exception {
    	MetadataStore metadataStore = new MetadataStore();
    	Schema foo = RealMetadataFactory.createPhysicalModel("foo", metadataStore); //$NON-NLS-1$
        Table table = RealMetadataFactory.createPhysicalGroup("bar", foo); //$NON-NLS-1$
        String[] elemNames = new String[] {
            "x"  //$NON-NLS-1$ 
        };
        String[] elemTypes = new String[] {  
            DataTypeManager.DefaultDataTypes.STRING
        };
        List<Column> cols =RealMetadataFactory.createElements(table, elemNames, elemTypes);
        
        Column obj = cols.get(0);
        obj.setNativeType("uniqueidentifier"); //$NON-NLS-1$
        
        CompositeMetadataStore store = new CompositeMetadataStore(metadataStore);
        QueryMetadataInterface metadata = new TransformationMetadata(null, store, null, RealMetadataFactory.SFM.getSystemFunctions(), null);
        
        TranslationUtility tu = new TranslationUtility(metadata);
        Command command = tu.parseCommand("select max(x) from bar"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("SELECT MAX(cast(bar.x as char(36))) FROM bar", trans, command); //$NON-NLS-1$
        
        command = tu.parseCommand("select * from (select max(x) as max from bar) x"); //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("SELECT x.max FROM (SELECT MAX(cast(bar.x as char(36))) AS max FROM bar) x", trans, command); //$NON-NLS-1$
    }
    
    @Test public void testRowLimitWithInlineViewOrderBy() throws Exception {
        String input = "select intkey from (select intkey from bqt1.smalla) as x order by intkey limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 v_0.c_0 FROM (SELECT g_0.IntKey AS c_0 FROM SmallA g_0) v_0 ORDER BY v_0.c_0"; //$NON-NLS-1$
               
		CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, trans, obj);
    }
    
    @Test public void testWith() throws Exception {
        String input = "with x as (select intkey from bqt1.smalla) select intkey from x limit 100"; //$NON-NLS-1$
        String output = "WITH x AS (SELECT SmallA.IntKey FROM SmallA) SELECT TOP 100 g_0.intkey AS c_0 FROM x g_0"; //$NON-NLS-1$
               
		CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, trans, obj);
    }
    
    @Test public void testParseFormat() throws Exception {
        String input = "select parsetimestamp(smalla.timestampvalue, 'yyyy.MM.dd'), formattimestamp(smalla.timestampvalue, 'yy.MM.dd') from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT CONVERT(DATETIME, convert(varchar, g_0.TimestampValue, 21), 102), CONVERT(VARCHAR, g_0.TimestampValue, 2) FROM SmallA g_0"; //$NON-NLS-1$
               
		CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, trans, obj);
    }
       
}
