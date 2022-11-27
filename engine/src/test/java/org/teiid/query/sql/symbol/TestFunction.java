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

package org.teiid.query.sql.symbol;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;

import junit.framework.TestCase;


public class TestFunction extends TestCase {

    // ################################## FRAMEWORK ################################

    public TestFunction(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    // ################################## ACTUAL TESTS ################################

    public void testFunction1() {
        Function f1 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        Function f2 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        UnitTestUtil.helpTestEquivalence(0, f1, f2);
    }

    public void testFunction2() {
        Function f1 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        Function f2 = new Function("F1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        UnitTestUtil.helpTestEquivalence(0, f1, f2);
    }

    public void testFunction3() {
        Function f1 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        Function f2 = new Function("f2", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        UnitTestUtil.helpTestEquivalence(1, f1, f2);
    }

    public void testFunction4() {
        Function f1 = new Function("f1", new Expression[] {null}); //$NON-NLS-1$
        Function f2 = new Function("f1", new Expression[] {null}); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(0, f1, f2);
    }

    public void testFunction5() {
        Function f1 = new Function("f1", new Expression[] {null}); //$NON-NLS-1$
        Function f2 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        UnitTestUtil.helpTestEquivalence(1, f1, f2);
    }

    public void testFunction6() {
        Function f1 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        Function f2 = new Function("f1", new Expression[] {new Constant("xyz"), new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        UnitTestUtil.helpTestEquivalence(1, f1, f2);
    }

}
