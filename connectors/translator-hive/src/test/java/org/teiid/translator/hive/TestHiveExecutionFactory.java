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
package org.teiid.translator.hive;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.CommandBuilder;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.metadata.*;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.TranslatedCommand;

@SuppressWarnings("nls")
public class TestHiveExecutionFactory {

    private static HiveExecutionFactory hiveTranslator; 
    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();
    private static TransformationMetadata bqt; 

    @BeforeClass
    public static void setUp() throws TranslatorException {
        hiveTranslator = new HiveExecutionFactory();
        hiveTranslator.setUseBindVariables(false);
        hiveTranslator.start();
        bqt = exampleBQT();
    }
    
    private void helpTest(Expression srcExpression, String tgtType, String expectedExpression) throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  Arrays.asList( srcExpression,LANG_FACTORY.createLiteral(tgtType, String.class)),TypeFacility.getDataTypeClass(tgtType));
        SQLConversionVisitor sqlVisitor = hiveTranslator.getSQLConversionVisitor(); 
        sqlVisitor.append(func); 
        assertEquals("Error converting from " + srcExpression.getType() + " to " + tgtType, expectedExpression,sqlVisitor.toString()); 
    }    
    
    private void helpTestVisitor(QueryMetadataInterface metadata, String input, String expectedOutput) throws TranslatorException {
        // Convert from sql to objects
        CommandBuilder commandBuilder = new CommandBuilder(metadata);
        Command obj = commandBuilder.getCommand(input);

        // Convert back to SQL
        TranslatedCommand tc = new TranslatedCommand(Mockito.mock(ExecutionContext.class), hiveTranslator);
        tc.translateCommand(obj);
        
        // Check stuff
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$		
    }
    
    @Test public void testConvertions() throws Exception {
    	helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), TypeFacility.RUNTIME_NAMES.BOOLEAN, "cast(true AS boolean)");
    	helpTest(LANG_FACTORY.createLiteral(Byte.parseByte("123"), Byte.class), TypeFacility.RUNTIME_NAMES.BYTE, "cast(123 AS tinyint)");
    	helpTest(LANG_FACTORY.createLiteral(new Integer(12345), Integer.class), TypeFacility.RUNTIME_NAMES.INTEGER, "cast(12345 AS int)");
    	helpTest(LANG_FACTORY.createLiteral(Short.parseShort("1234"), Short.class), TypeFacility.RUNTIME_NAMES.SHORT, "cast(1234 AS smallint)");
    	helpTest(LANG_FACTORY.createLiteral(new BigInteger("123451266182"), BigInteger.class), TypeFacility.RUNTIME_NAMES.BIG_INTEGER, "cast(123451266182 AS bigint)");
    	helpTest(LANG_FACTORY.createLiteral(new String("foo-bar"), String.class), TypeFacility.RUNTIME_NAMES.STRING, "cast('foo-bar' AS string)");
    	helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), TypeFacility.RUNTIME_NAMES.STRING, "cast(true AS string)");
    	helpTest(LANG_FACTORY.createLiteral(new Integer(12345), Integer.class), TypeFacility.RUNTIME_NAMES.BOOLEAN, "cast(12345 AS boolean)");
    }
    
    @Test
    public void testFunction() throws Exception {
        String input = "SELECT MOD(A.intkey,2) FROM BQT1.SMALLA A"; 
        String output = "SELECT (A.IntKey % 2) FROM SmallA A"; 
        helpTestVisitor(bqt, input, output);
    }
    
    @Test public void testTimeLiterals() throws Exception {
        String input = "SELECT {ts '1999-01-01 11:11:11'}, {d '2000-02-02'}, {t '00:00:00'} FROM BQT1.SMALLA A"; 
        String output = "SELECT '1999-01-01 11:11:11.0', '2000-02-02', '1970-01-01 00:00:00.0' FROM SmallA A"; 
        helpTestVisitor(bqt, input, output);
    }
    
    @Test
    public void testEqualityJoinCriteria() throws Exception {
        String input = "SELECT A.intkey FROM BQT1.SMALLA A JOIN BQT1.SmallB B on A.intkey=B.intkey"; 
        String output = "SELECT A.IntKey FROM SmallA A  JOIN SmallB B ON A.IntKey = B.IntKey"; 
        helpTestVisitor(bqt, input, output);
    }
    
    @Test
    public void testCrossJoinCriteria() throws Exception {
        String input = "SELECT A.intkey FROM BQT1.SMALLA A Cross join BQT1.SmallB B"; 
        String output = "SELECT A.IntKey FROM SmallA A CROSS JOIN SmallB B"; 
        helpTestVisitor(bqt, input, output);
    }
    
    
    @Test
    public void testMustHaveAliasOnView() throws Exception {
        String input = "SELECT intkey FROM (select intkey from BQT1.SmallA) as X"; 
        String output = "SELECT X.intkey FROM (SELECT SmallA.IntKey FROM SmallA) X"; 
        helpTestVisitor(bqt, input, output);
    }
    
    @Test
    public void testUnionAllRewrite() throws Exception {
        String input = "SELECT intkey, stringkey FROM BQT1.SmallA union all SELECT intkey, stringkey FROM BQT1.Smallb"; 
        String output = "SELECT intkey, stringkey FROM (SELECT SmallA.IntKey, SmallA.StringKey FROM SmallA UNION ALL SELECT SmallB.IntKey, SmallB.StringKey FROM SmallB) X__"; 
        helpTestVisitor(bqt, input, output);    	
    }
    
    @Test
    public void testUnionAllExprRewrite() throws Exception {
        String input = "SELECT count(*) as key, stringkey FROM BQT1.SmallA union all SELECT intkey, stringkey FROM BQT1.Smallb"; 
        String output = "SELECT key, stringkey FROM (SELECT COUNT(*) AS key, SmallA.StringKey FROM SmallA UNION ALL SELECT SmallB.IntKey, SmallB.StringKey FROM SmallB) X__"; 
        helpTestVisitor(bqt, input, output);    	
    }
    
    @Test
    public void testUnionRewrite() throws Exception {
        String input = "SELECT intkey, stringkey FROM BQT1.SmallA union SELECT intkey, stringkey FROM BQT1.Smallb"; 
        String output = "SELECT DISTINCT intkey, stringkey FROM (SELECT SmallA.IntKey, SmallA.StringKey FROM SmallA UNION ALL SELECT SmallB.IntKey, SmallB.StringKey FROM SmallB) X__"; 
        helpTestVisitor(bqt, input, output);    	
    }
    
    @Test
    public void testGroupByOrderBy() throws Exception {
        String input = "SELECT intkey FROM BQT1.SmallA group by intkey order by intkey"; 
        String output = "SELECT SmallA.IntKey FROM SmallA GROUP BY SmallA.IntKey ORDER BY IntKey"; 
        helpTestVisitor(bqt, input, output);    	
    }
    
    public static TransformationMetadata exampleBQT() {
    	MetadataStore metadataStore = new MetadataStore();
    	Schema bqt1 = RealMetadataFactory.createPhysicalModel("BQT1", metadataStore); //$NON-NLS-1$
    	Table bqt1SmallA = RealMetadataFactory.createPhysicalGroup("SmallA", bqt1); //$NON-NLS-1$
    	Table bqt1SmallB = RealMetadataFactory.createPhysicalGroup("SmallB", bqt1); //$NON-NLS-1$
        String[] elemNames = new String[] { 
                "IntKey", "StringKey",  //$NON-NLS-1$ //$NON-NLS-2$
                "IntNum", "StringNum",  //$NON-NLS-1$ //$NON-NLS-2$
                "FloatNum", "LongNum",  //$NON-NLS-1$ //$NON-NLS-2$
                "DoubleNum", "ByteNum",  //$NON-NLS-1$ //$NON-NLS-2$
                "DateValue", "TimeValue",  //$NON-NLS-1$ //$NON-NLS-2$
                "TimestampValue", "BooleanValue",  //$NON-NLS-1$ //$NON-NLS-2$
                "CharValue", "ShortValue",  //$NON-NLS-1$ //$NON-NLS-2$
                "BigIntegerValue", "BigDecimalValue",  //$NON-NLS-1$ //$NON-NLS-2$
                "ObjectValue" }; //$NON-NLS-1$
        
        String[] nativeTypes = new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, 
                DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, 
                DataTypeManager.DefaultDataTypes.FLOAT, DataTypeManager.DefaultDataTypes.BIG_INTEGER, 
                DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.BYTE, 
                DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, 
                DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.DefaultDataTypes.BOOLEAN, 
                DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.SHORT, 
                DataTypeManager.DefaultDataTypes.BIG_INTEGER, DataTypeManager.DefaultDataTypes.BIG_INTEGER, 
                DataTypeManager.DefaultDataTypes.STRING};
        
            String[] runtimeTypes = new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, 
                                DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, 
                                DataTypeManager.DefaultDataTypes.FLOAT, DataTypeManager.DefaultDataTypes.LONG, 
                                DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.BYTE, 
                                DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIME, 
                                DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.DefaultDataTypes.BOOLEAN, 
                                DataTypeManager.DefaultDataTypes.CHAR, DataTypeManager.DefaultDataTypes.SHORT, 
                                DataTypeManager.DefaultDataTypes.BIG_INTEGER, DataTypeManager.DefaultDataTypes.BIG_DECIMAL, 
                                DataTypeManager.DefaultDataTypes.OBJECT };
           
           List<Column> bqt1SmallAe = RealMetadataFactory.createElements(bqt1SmallA, elemNames, nativeTypes);
           List<Column> bqt1SmallBe = RealMetadataFactory.createElements(bqt1SmallB, elemNames, nativeTypes);
           
           Schema vqt = RealMetadataFactory.createVirtualModel("VQT", metadataStore); //$NON-NLS-1$
           QueryNode vqtn1 = new QueryNode("SELECT * FROM BQT1.SmallA"); //$NON-NLS-1$ 
           Table vqtg1 = RealMetadataFactory.createUpdatableVirtualGroup("SmallA", vqt, vqtn1); //$NON-NLS-1$
           RealMetadataFactory.createElements(vqtg1, elemNames, runtimeTypes); 
           
           return RealMetadataFactory.createTransformationMetadata(metadataStore, "bqt");//$NON-NLS-1$
    }
    
    @Test public void testExcludeTables() throws Exception {
    	HiveMetadataProcessor hmp = new HiveMetadataProcessor();
    	hmp.setExcludeTables("x");
    	Connection c = Mockito.mock(Connection.class);
    	MetadataFactory mf = Mockito.mock(MetadataFactory.class);
    	Statement stmt = Mockito.mock(Statement.class);
    	Mockito.stub(c.createStatement()).toReturn(stmt);
    	ResultSet rs = Mockito.mock(ResultSet.class);
    	Mockito.stub(stmt.executeQuery("SHOW TABLES")).toReturn(rs);
    	Mockito.stub(rs.next()).toReturn(true).toReturn(false);
    	Mockito.stub(rs.getString(1)).toReturn("x");
    	
    	hmp.process(mf, c);
    	Mockito.verify(mf, Mockito.times(0)).addTable("x");
    }
}
