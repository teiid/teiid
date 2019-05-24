/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.teiid.language.Select;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.TranslationHelper;

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

    @Test public void testOffset() {
        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.example1Cached());
        Command obj = commandBuilder.getCommand("select pm1.g1.e2 from pm1.g1 limit 1, 2");
        SQLConversionVisitor sqlVisitor = impalaTranslator.getSQLConversionVisitor();
        sqlVisitor.append(obj);
        assertEquals("SELECT g1.e2 FROM g1 ORDER BY 1 LIMIT 2 OFFSET 1", sqlVisitor.toString());

        obj = commandBuilder.getCommand("select pm1.g1.e2, pm1.g1.e1 from pm1.g1 order by e1 limit 1, 100");
        sqlVisitor = impalaTranslator.getSQLConversionVisitor();
        sqlVisitor.append(obj);
        assertEquals("SELECT g1.e2, g1.e1 FROM g1 ORDER BY e1 LIMIT 100 OFFSET 1", sqlVisitor.toString());
    }

    @Test public void testOrderedUnion() {
        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand("SELECT g_1.StringNum AS c_0 FROM bqt1.SmallA AS g_1 WHERE g_1.IntKey <= 50 UNION ALL SELECT g_0.StringNum AS c_0 FROM bqt1.SmallB AS g_0 WHERE g_0.IntKey > 50 ORDER BY c_0 limit 10");
        SQLConversionVisitor sqlVisitor = impalaTranslator.getSQLConversionVisitor();
        sqlVisitor.append(obj);
        assertEquals("SELECT c_0 FROM (SELECT g_1.StringNum AS c_0 FROM SmallA g_1 WHERE g_1.IntKey <= 50 UNION ALL SELECT g_0.StringNum AS c_0 FROM SmallB g_0 WHERE g_0.IntKey > 50) X__ ORDER BY c_0 LIMIT 10", sqlVisitor.toString());
    }

    @Test public void testDistinctAggregate() {
        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand("SELECT distinct max(StringNum) FROM bqt1.SmallA group by stringkey");
        SQLConversionVisitor sqlVisitor = impalaTranslator.getSQLConversionVisitor();
        sqlVisitor.append(obj);
        assertEquals("SELECT DISTINCT c_0 FROM (SELECT MAX(SmallA.StringNum) AS c_0 FROM SmallA GROUP BY SmallA.StringKey) X__", sqlVisitor.toString());
    }

    @Test public void testWith() {
        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Select obj = (Select) commandBuilder.getCommand("with x as /*+ no_inline */ (SELECT max(StringNum) as a FROM bqt1.SmallA group by stringkey) select * from x");
        SQLConversionVisitor sqlVisitor = impalaTranslator.getSQLConversionVisitor();
        sqlVisitor.append(obj);
        assertEquals("WITH x AS (SELECT MAX(SmallA.StringNum) AS a FROM SmallA GROUP BY SmallA.StringKey) SELECT x.a FROM x", sqlVisitor.toString());
    }

    @Test public void testPredicateFunctions() {
        Select obj = (Select)TranslationHelper.helpTranslate("/bqt.vdb", impalaTranslator.getPushDownFunctions(), "select stringnum FROM bqt1.SmallA where ilike(stringkey, 'a_') and not(ilike(stringkey, '_b'))");
        SQLConversionVisitor sqlVisitor = impalaTranslator.getSQLConversionVisitor();
        sqlVisitor.append(obj);
        assertEquals("SELECT SmallA.StringNum FROM SmallA WHERE (SmallA.StringKey ilike 'a_') AND NOT((SmallA.StringKey ilike '_b'))", sqlVisitor.toString());
    }

    @Test public void testLikeRegex() {
        CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Select obj = (Select) commandBuilder.getCommand("SELECT max(StringNum) as a FROM bqt1.SmallA where stringkey like_regex '^[1-9]$'");
        SQLConversionVisitor sqlVisitor = impalaTranslator.getSQLConversionVisitor();
        sqlVisitor.append(obj);
        assertEquals("SELECT MAX(SmallA.StringNum) AS a FROM SmallA WHERE SmallA.StringKey REGEXP '^[1-9]$'", sqlVisitor.toString());
    }
}
