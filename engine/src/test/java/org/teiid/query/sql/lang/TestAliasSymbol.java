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

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ExpressionSymbol;


public class TestAliasSymbol {

    @Test public void testAliasEquals() {
        AliasSymbol a1 = new AliasSymbol("X", new ExpressionSymbol("x", new Constant(1))); //$NON-NLS-1$ //$NON-NLS-2$
        AliasSymbol a2 = new AliasSymbol("X", new ExpressionSymbol("x", new Constant(2))); //$NON-NLS-1$ //$NON-NLS-2$
        AliasSymbol a3 = new AliasSymbol("x", new ExpressionSymbol("x", new Constant(1))); //$NON-NLS-1$ //$NON-NLS-2$

        assertFalse(a1.equals(a3)); //just a different case for the alias

        assertFalse(a1.equals(a2)); //different express
    }

    @Test public void testClone() {
        AliasSymbol a1 = new AliasSymbol("X", new ExpressionSymbol("x", new Constant(1))); //$NON-NLS-1$ //$NON-NLS-2$
        a1.setOutputName("foo"); //$NON-NLS-1$
        AliasSymbol clone = (AliasSymbol)a1.clone();
        assertEquals(a1, clone);
        assertEquals(a1.getOutputName(), clone.getOutputName());
    }

}
