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

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.cdk.CommandBuilder;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.SQLConversionVisitor;

@SuppressWarnings("nls")
public class TestImpalaExecutionFactory {

    private static ImpalaExecutionFactory impalaTranslator; 
    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();
    private static TransformationMetadata bqt; 

    @BeforeClass
    public static void setUp() throws TranslatorException {
        impalaTranslator = new ImpalaExecutionFactory();
        impalaTranslator.setUseBindVariables(false);
        impalaTranslator.start();
        bqt = TestHiveExecutionFactory.exampleBQT();
    }
    
    private void helpTest(Expression srcExpression, String tgtType, String expectedExpression) throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  Arrays.asList( srcExpression,LANG_FACTORY.createLiteral(tgtType, String.class)),TypeFacility.getDataTypeClass(tgtType));
        SQLConversionVisitor sqlVisitor = impalaTranslator.getSQLConversionVisitor(); 
        sqlVisitor.append(func); 
        assertEquals("Error converting from " + srcExpression.getType() + " to " + tgtType, expectedExpression,sqlVisitor.toString()); 
    }    
    
   
    @Test public void testConvertions() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(12345), Integer.class), TypeFacility.RUNTIME_NAMES.DOUBLE, "cast(12345 AS double)");
    }
    
    @Test public void testConversionSupport() {
    	assertFalse(impalaTranslator.supportsConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME));
    	assertTrue(impalaTranslator.supportsConvert(FunctionModifier.STRING, FunctionModifier.TIMESTAMP));
    }
    
    @Test public void testJoin() {
    	CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.example1Cached());
        Command obj = commandBuilder.getCommand("select pm1.g1.e1 from pm1.g1 inner join pm1.g2 inner join pm1.g3 on pm1.g2.e2 = pm1.g3.e2 on pm1.g1.e1 = pm1.g2.e1");
        SQLConversionVisitor sqlVisitor = impalaTranslator.getSQLConversionVisitor(); 
        sqlVisitor.append(obj);
        assertEquals("SELECT g1.e1 FROM g2  JOIN g3 ON g2.e2 = g3.e2  JOIN g1 ON g1.e1 = g2.e1", sqlVisitor.toString());
    }
    
    @Test public void testStringLiteral() {
    	CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.example1Cached());
        Command obj = commandBuilder.getCommand("select pm1.g1.e2 from pm1.g1 where pm1.g1.e1 = 'a''b\\c'");
        SQLConversionVisitor sqlVisitor = impalaTranslator.getSQLConversionVisitor(); 
        sqlVisitor.append(obj);
        assertEquals("SELECT g1.e2 FROM g1 WHERE g1.e1 = 'a\\'b\\\\c'", sqlVisitor.toString());
    }
    
    @Test public void testMultipleDistinctAggregates() {
    	CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.example1Cached());
        Command obj = commandBuilder.getCommand("select count(distinct pm1.g1.e1), 1, count(distinct pm1.g1.e2), avg(distinct pm1.g1.e4) from pm1.g1");
        SQLConversionVisitor sqlVisitor = impalaTranslator.getSQLConversionVisitor(); 
        sqlVisitor.append(obj);
        assertEquals("SELECT v0.c0, v0.c1, v1.c2, v2.c3 FROM (SELECT COUNT(DISTINCT g1.e1) AS c0, 1 AS c1 FROM g1) v0 CROSS JOIN (SELECT COUNT(DISTINCT g1.e2) AS c2 FROM g1) v1 CROSS JOIN (SELECT AVG(DISTINCT g1.e4) AS c3 FROM g1) v2", sqlVisitor.toString());
        
        obj = commandBuilder.getCommand("select count(distinct pm1.g1.e1), 1, count(distinct pm1.g1.e2), avg(distinct pm1.g1.e4) from pm1.g1 where pm1.g1.e3 = true");
        sqlVisitor = impalaTranslator.getSQLConversionVisitor();
        sqlVisitor.append(obj);
        assertEquals("SELECT v0.c0, v0.c1, v1.c2, v2.c3 FROM (SELECT COUNT(DISTINCT g1.e1) AS c0, 1 AS c1 FROM g1 WHERE g1.e3 = true) v0 CROSS JOIN (SELECT COUNT(DISTINCT g1.e2) AS c2 FROM g1 WHERE g1.e3 = true) v1 CROSS JOIN (SELECT AVG(DISTINCT g1.e4) AS c3 FROM g1 WHERE g1.e3 = true) v2", sqlVisitor.toString());
    }
}
