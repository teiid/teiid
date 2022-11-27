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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.util.Collections;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.xml.sax.SAXException;

@SuppressWarnings("nls")
public class TestVDBMetadataParser {

    @Test
    public void testParseVDB() throws Exception {
        FileInputStream in = new FileInputStream(UnitTestUtil.getTestDataPath() + "/parser-test-vdb.xml");
        VDBMetadataParser.validate(in);
        in = new FileInputStream(UnitTestUtil.getTestDataPath() + "/parser-test-vdb.xml");
        VDBMetaData vdb = VDBMetadataParser.unmarshall(in);
        TestVDBUtility.validateVDB(vdb);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        VDBMetadataParser.marshall(vdb, baos);
        baos.close();
        VDBMetaData parsed = VDBMetadataParser.unmarshall(new ByteArrayInputStream(baos.toByteArray()));

        TestVDBUtility.validateVDB(parsed);
    }

    @Test public void testExcludeImported() throws Exception {
        VDBMetaData metadata = TestVDBUtility.buildVDB();
        assertNotNull(metadata.getModel("model-one"));
        metadata.setImportedModels(Collections.singleton("model-one"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        VDBMetadataParser.marshall(metadata, baos);
        baos.close();
        VDBMetaData parsed = VDBMetadataParser.unmarshall(new ByteArrayInputStream(baos.toByteArray()));
        assertNull(parsed.getModel("model-one"));
    }

    @Test(expected=SAXException.class) public void testModelNameUniqueness() throws Exception {
        FileInputStream in = new FileInputStream(UnitTestUtil.getTestDataPath() + "/model-not-unique-vdb.xml");
        VDBMetadataParser.validate(in);
    }

}
