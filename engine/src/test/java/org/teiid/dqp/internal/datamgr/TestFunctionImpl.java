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

package org.teiid.dqp.internal.datamgr;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.query.resolver.TestFunctionResolving;
import org.teiid.query.sql.symbol.Constant;

@SuppressWarnings("nls")
public class TestFunctionImpl {

    public static org.teiid.query.sql.symbol.Function helpExample(String name) {
        Constant c1 = new Constant(new Integer(100));
        Constant c2 = new Constant(new Integer(200));
        org.teiid.query.sql.symbol.Function f = new org.teiid.query.sql.symbol.Function(name, new org.teiid.query.sql.symbol.Expression[] {c1, c2});
        f.setType(Integer.class);
        return f;
    }

    public static Function example(String name) throws Exception {
        return (Function) TstLanguageBridgeFactory.factory.translate(helpExample(name));
    }

    @Test public void testGetName() throws Exception {
        assertEquals("testName", example("testName").getName()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testGetParameters() throws Exception {
        List<Expression> params = example("testFunction").getParameters(); //$NON-NLS-1$
        assertNotNull(params);
        assertEquals(2, params.size());
        for (int i = 0; i < params.size(); i++) {
            assertNotNull(params.get(i));
        }
    }

    @Test public void testGetType() throws Exception {
        assertEquals(Integer.class, example("test").getType()); //$NON-NLS-1$
    }

    @Test(expected=TeiidRuntimeException.class) public void testLongTimestampAddLiteral() throws Exception {
        org.teiid.query.sql.symbol.Expression ex = TestFunctionResolving.getExpression("timestampadd(sql_tsi_second, 9999999999, now())");
        TstLanguageBridgeFactory.factory.translate(ex);
    }

    @Test public void testLongTimestampAddLiteral1() throws Exception {
        org.teiid.query.sql.symbol.Function ex = (org.teiid.query.sql.symbol.Function)TestFunctionResolving.getExpression("timestampadd(sql_tsi_second, 1, now())");
        ex.getArgs()[1] = new Constant(1L);
        Function f = (Function) TstLanguageBridgeFactory.factory.translate(ex);
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, f.getParameters().get(1).getType());
    }

    @Test public void testLongTimestampAddLongExpression() throws Exception {
        org.teiid.query.sql.symbol.Function ex = (org.teiid.query.sql.symbol.Function)TestFunctionResolving.getExpression("timestampadd(sql_tsi_second, cast(1 as long), now())");
        Function f = (Function) TstLanguageBridgeFactory.factory.translate(ex);
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, f.getParameters().get(1).getType());
    }

}
