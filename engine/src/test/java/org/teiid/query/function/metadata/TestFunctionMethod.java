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

package org.teiid.query.function.metadata;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;


public class TestFunctionMethod {

    @Test public void testEquivalence1() {
        FunctionParameter p1 = new FunctionParameter("in", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionParameter pout = new FunctionParameter("out", "string"); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionMethod m1 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length",  //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] { p1 }, pout );

        UnitTestUtil.helpTestEquivalence(0, m1, m1);
    }

    @Test public void testEquivalence11() {
        FunctionParameter pout = new FunctionParameter("out", "string"); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionMethod m1 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length",  //$NON-NLS-1$ //$NON-NLS-2$
            null, pout );

        UnitTestUtil.helpTestEquivalence(0, m1, m1);
    }

    @Test public void testEquivalence2() {
        FunctionParameter p1 = new FunctionParameter("in", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionParameter pout = new FunctionParameter("out", "string"); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionMethod m1 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length", //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] { p1 }, pout );

        FunctionParameter p2 = new FunctionParameter("in", "integer"); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionParameter pout2 = new FunctionParameter("out", "string"); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionMethod m2 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length", //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] { p2 }, pout2 );

        UnitTestUtil.helpTestEquivalence(1, m1, m2);
    }

    @Test public void testEquivalence3() {
        FunctionParameter p1 = new FunctionParameter("in", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionParameter pout = new FunctionParameter("out", "string"); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionMethod m1 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length", //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] { p1 }, pout );

        FunctionParameter p2 = new FunctionParameter("in", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionParameter pout2 = new FunctionParameter("out", "integer"); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionMethod m2 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length", //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] { p2 }, pout2 );

        UnitTestUtil.helpTestEquivalence(0, m1, m2);
    }

}
