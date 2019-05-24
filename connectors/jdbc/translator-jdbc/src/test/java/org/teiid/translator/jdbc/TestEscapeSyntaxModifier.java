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

package org.teiid.translator.jdbc;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.teiid.cdk.CommandBuilder;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.jdbc.EscapeSyntaxModifier;

import junit.framework.TestCase;


/**
 */
@SuppressWarnings("nls")
public class TestEscapeSyntaxModifier extends TestCase {

    /**
     * Constructor for TestDropFunctionModifier.
     * @param name
     */
    public TestEscapeSyntaxModifier(String name) {
        super(name);
    }

    public void testEscape() {
        Literal arg1 = CommandBuilder.getLanuageFactory().createLiteral("arg1", String.class); //$NON-NLS-1$
        Literal arg2 = CommandBuilder.getLanuageFactory().createLiteral("arg2", String.class);//$NON-NLS-1$
        Function func = CommandBuilder.getLanuageFactory().createFunction("concat", Arrays.asList( arg1, arg2), Integer.class); //$NON-NLS-1$

        helpTest(func, "{fn concat('arg1', 'arg2')}");
    }

    public void testTimestampAdd() {
        Literal arg1 = CommandBuilder.getLanuageFactory().createLiteral(NonReserved.SQL_TSI_HOUR, String.class);
        Literal arg2 = CommandBuilder.getLanuageFactory().createLiteral(Integer.valueOf(1), Integer.class);
        Literal arg3 = CommandBuilder.getLanuageFactory().createLiteral(TimestampUtil.createTimestamp(0, 0, 0, 0, 0, 0, 0), Timestamp.class);
        Function func = CommandBuilder.getLanuageFactory().createFunction("timestampadd", Arrays.asList( arg1, arg2, arg3), Timestamp.class); //$NON-NLS-1$

        helpTest(func, "{fn timestampadd(SQL_TSI_HOUR, 1, {ts '1899-12-31 00:00:00.0'})}");
    }

    private void helpTest(Function func, String expected) {
        EscapeSyntaxModifier mod = new EscapeSyntaxModifier();

        List parts = mod.translate(func);
        StringBuffer sb = new StringBuffer();
        for (Object object : parts) {
            sb.append(object);
        }
        assertEquals(expected, sb.toString());
    }

}
