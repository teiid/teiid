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

package org.teiid.query.sql.lang;

import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;

import junit.framework.TestCase;

public class TestExpressionSymbol extends TestCase {


    public void testExpressionHashCode() {
        Expression expr1 = new Constant(new Integer(1));
        Expression expr2 = new Constant(new Integer(2));
        ExpressionSymbol symbol1 = new ExpressionSymbol("foo", expr1); //$NON-NLS-1$
        ExpressionSymbol symbol2 = new ExpressionSymbol("bar", expr2); //$NON-NLS-1$

        assertFalse(symbol1.hashCode() == symbol2.hashCode());
    }

    public void testExpressionHashCode1() {
        Expression expr1 = new Constant(new Integer(1));
        Expression expr2 = new Constant(new Integer(1));
        ExpressionSymbol symbol1 = new ExpressionSymbol("foo", expr1); //$NON-NLS-1$
        ExpressionSymbol symbol2 = new ExpressionSymbol("bar", expr2); //$NON-NLS-1$

        assertTrue(symbol1.hashCode() == symbol2.hashCode());
    }


}
