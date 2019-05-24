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

package org.teiid.adminapi.impl;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Model.Type;

@SuppressWarnings("nls")
public class TestModelMetaData {

    @Test
    public void testModelType() {

        ModelMetaData model = new ModelMetaData();
        model.setModelType("physical");

        assertTrue(model.getModelType() == Model.Type.PHYSICAL);
        assertTrue(model.isSource());

        model.modelType = "VIRTUAL";
        assertTrue(model.getModelType() == Model.Type.VIRTUAL);

        model.modelType = "TYPE";
        assertTrue(model.getModelType() == Model.Type.OTHER);
        assertTrue(!model.isSource());
    }

    @Test
    public void testSupportMultiSource() {
        ModelMetaData model = new ModelMetaData();
        assertFalse(model.isSupportsMultiSourceBindings());
        model.setSupportsMultiSourceBindings(true);

        assertTrue(model.isSupportsMultiSourceBindings());

        model.setModelType(Type.VIRTUAL);

        assertFalse(model.isSupportsMultiSourceBindings());

        assertTrue(!model.getProperties().isEmpty());
    }

    public void testErrors() {
        ModelMetaData m = new ModelMetaData();
        m.addMessage("ERROR", "I am Error");
        m.addMessage("WARNING", "I am warning");

        assertFalse(m.getMessages().isEmpty());
        assertEquals(1, m.getMessages().size());
    }
}
