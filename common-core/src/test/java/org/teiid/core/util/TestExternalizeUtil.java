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

package org.teiid.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.util.ExternalizeUtil;

import junit.framework.TestCase;

public class TestExternalizeUtil extends TestCase {

    private ByteArrayOutputStream bout;
    private ObjectOutputStream oout;

    /**
     * Constructor for TestExternalizeUtil.
     * @param name
     */
    public TestExternalizeUtil(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        bout = new ByteArrayOutputStream(4096);
        oout = new ObjectOutputStream(bout);
    }

    public void testEmptyCollection() throws Exception {
        ExternalizeUtil.writeCollection(oout, Arrays.asList(new Object[0]));
        oout.flush();
        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        ObjectInputStream oin = new ObjectInputStream(bin);

        List<?> result = ExternalizeUtil.readList(oin);
        assertEquals(0, result.size());
    }

}
