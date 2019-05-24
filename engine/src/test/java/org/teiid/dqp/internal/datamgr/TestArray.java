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

import org.junit.Test;
import org.teiid.core.types.ArrayImpl;
import org.teiid.language.Array;
import org.teiid.query.sql.symbol.Constant;


public class TestArray {

    public static Array example(Object... values) throws Exception {
        ArrayImpl array = new ArrayImpl(values);
        return (Array) TstLanguageBridgeFactory.factory.translate(new Constant(array));

    }

    @Test
    public void testGetBaseType() throws Exception {
         Array array = example(null, "a"); //$NON-NLS-1$
         assertEquals(String.class, array.getBaseType());
    }

}
