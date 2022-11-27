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

package org.teiid.translator.jdbc.oracle;

import java.sql.Timestamp;
import java.util.Arrays;

import junit.framework.TestCase;

import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.jdbc.SQLConversionVisitor;


/**
 */
public class TestMonthOrDayNameFunctionModifier extends TestCase {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    /**
     * Constructor for TestHourFunctionModifier.
     * @param name
     */
    public TestMonthOrDayNameFunctionModifier(String name) {
        super(name);
    }

    public void helpTestMod(Literal c, String format, String expectedStr) throws Exception {
        Function func = LANG_FACTORY.createFunction(format.toLowerCase()+"name",  // "monthname" //$NON-NLS-1$
            Arrays.asList( c ),
            String.class);

        OracleExecutionFactory trans = new OracleExecutionFactory();
        trans.start();

        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor();
        sqlVisitor.append(func);
        assertEquals(expectedStr, sqlVisitor.toString());
    }

    public void test1() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 10, 5, 0, 10000000), Timestamp.class);
        helpTestMod(arg1, "Month", //$NON-NLS-1$
            "rtrim(TO_CHAR({ts '2004-01-21 10:05:00.01'}, 'Month'))"); //$NON-NLS-1$
    }

    public void test2() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        helpTestMod(arg1, "Month", //$NON-NLS-1$
            "rtrim(TO_CHAR({d '2004-01-21'}, 'Month'))"); //$NON-NLS-1$
    }

    public void test3() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(104, 0, 21, 10, 5, 0, 10000000), Timestamp.class);
        helpTestMod(arg1, "Day",  //$NON-NLS-1$
            "rtrim(TO_CHAR({ts '2004-01-21 10:05:00.01'}, 'Day'))"); //$NON-NLS-1$
    }

    public void test4() throws Exception {
        Literal arg1 = LANG_FACTORY.createLiteral(TimestampUtil.createDate(104, 0, 21), java.sql.Date.class);
        helpTestMod(arg1, "Day", //$NON-NLS-1$
            "rtrim(TO_CHAR({d '2004-01-21'}, 'Day'))"); //$NON-NLS-1$
    }
}