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
/**
 * Basic testing class for Apache Druid Translator.
 * Created by Don Krapohl 04/02/2021
 */
package org.teiid.translator.jdbc.druid;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.CommandBuilder;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.ExecutionContextImpl;
import org.teiid.dqp.internal.datamgr.FakeExecutionContextImpl;
import org.teiid.language.Array;
import org.teiid.language.*;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.metadata.*;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.*;
import org.teiid.translator.jdbc.oracle.OracleExecutionFactory;
import org.teiid.translator.jdbc.oracle.OracleMetadataProcessor;
import org.teiid.util.Version;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.teiid.language.LanguageFactory;
@SuppressWarnings("nls")
public class TestDruidTranslator {
	
    private DruidExecutionFactory TRANSLATOR;
    private static ExecutionContext EMPTY_CONTEXT = new FakeExecutionContextImpl();
    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    @Before 
    public void setup() throws Exception {
        TRANSLATOR = new DruidExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.setDatabaseVersion(Version.DEFAULT_VERSION);
        TRANSLATOR.start();
    }

    private String getTestVDB() {
        return TranslationHelper.PARTS_VDB;
    }

    private String getTestBQTVDB() {
        return TranslationHelper.BQT_VDB;
    }

    public void helpTestVisitor(String vdb, String input, String expectedOutput) throws TranslatorException {
        TranslationHelper.helpTestVisitor(vdb, input, expectedOutput, TRANSLATOR);
    }
    @Test
    public void testPushDownFuctions() throws TranslatorException {

        String input = "SELECT reverse('abc') FROM BQT1.MediumA"; //$NON-NLS-1$
        String output = "SELECT reverse('abc') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getTestBQTVDB(), input, output);
    }

    @Test
    public void testStringConversion() throws TranslatorException {

        String input = "SELECT lcase(part_name) FROM Parts"; //$NON-NLS-1$
        String output = "SELECT lower(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
        helpTestVisitor(getTestVDB(), input, output);

    }

    @Test
    public void testTimeFunctions() throws TranslatorException {

        String input = "select FORMATTIMESTAMP(timestampvalue, 'yyyy-MM-dd') from BQT1.MediumA"; //$NON-NLS-1$
        String output = "SELECT TIME_FORMAT(MediumA.TimestampValue, 'yyyy-MM-dd') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getTestBQTVDB(), input, output);

        input = "select PARSETIMESTAMP(formattimestamp(timestampvalue, 'yyyy-MM-dd'), 'yyyy-MM-dd') from BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT TIME_PARSE(TIME_FORMAT(MediumA.TimestampValue, 'yyyy-MM-dd'), 'yyyy-MM-dd') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getTestBQTVDB(), input, output);

        input = "select TIMESTAMPADD(SQL_TSI_DAY, 1, timestampvalue) from BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT MediumA.TimestampValue + (INTERVAL '1' DAY(1)) FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getTestBQTVDB(), input, output);

        //input = "select TIMESTAMPDIFF(SQL_TSI_DAY, NOW(), timestampvalue) from BQT1.MediumA"; //$NON-NLS-1$
        //output = "SELECT MediumA.TimestampValue + (INTERVAL '1' DAY(1)) FROM MediumA"; //$NON-NLS-1$
        //helpTestVisitor(getTestBQTVDB(), input, output);
    }
    @Test
    public void testStringToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29 23:59:59.987", String.class), "timestamp",
                "TIME_PARSE('2004-06-29 23:59:59.987', 'YYYY-MM-DD HH24:MI:SS.FF')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void helpTest(Expression srcExpression, String tgtType, String expectedExpression) throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
                Arrays.asList(
                        srcExpression,
                        LANG_FACTORY.createLiteral(tgtType, String.class)),
                TypeFacility.getDataTypeClass(tgtType));

        assertEquals("Error converting from " + srcExpression.getType() + " to " + tgtType, //$NON-NLS-1$ //$NON-NLS-2$
                expectedExpression, helpGetString(func));
    }
    public String helpGetString(Expression expr) throws Exception {
        DruidExecutionFactory trans = new DruidExecutionFactory();
        trans.start();

        SQLConversionVisitor sqlVisitor = TRANSLATOR.getSQLConversionVisitor();
        sqlVisitor.append(expr);

        return sqlVisitor.toString();
    }

}
