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
package org.teiid.adminapi.jboss;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.descriptions.NonResolvingResourceDescriptionResolver;
import org.jboss.dmr.ModelNode;
import org.junit.Test;
import org.teiid.adminapi.impl.TestVDBUtility;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestVDBMetaData {

    @Test
    public void testMarshellUnmarshallDirectParsing() throws Exception {

        VDBMetaData vdb = TestVDBUtility.buildVDB();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        VDBMetadataParser.marshall(vdb, out);

        //System.out.println(new String(out.toByteArray()));

        // Unmarshall
        vdb = VDBMetadataParser.unmarshall(new ByteArrayInputStream(out.toByteArray()));

        TestVDBUtility.validateVDB(vdb);
    }



    @Test
    public void testAdminMOCreation() {
        VDBMetaData vdb = new VDBMetaData();

        PropertiesUtils.setBeanProperty(vdb, "name", "x");

        assertEquals("x", vdb.getName());
    }

    @Test public void testVDBMetaDataMapper() {
        VDBMetaData vdb = TestVDBUtility.buildVDB();

        ModelNode node = VDBMetadataMapper.INSTANCE.wrap(vdb, new ModelNode());

        vdb = VDBMetadataMapper.INSTANCE.unwrap(node);
        TestVDBUtility.validateVDB(vdb);
    }

    @Test
    public void testVDBMetaDataDescribe() throws Exception {
        ModelNode node = TestVDBMetaData.describe(new ModelNode(), VDBMetadataMapper.INSTANCE.getAttributeDefinitions());
        String actual = node.toJSONString(false);

        assertEquals(ObjectConverterUtil.convertFileToString(new File(UnitTestUtil.getTestDataPath() + "/vdb-describe.txt")), actual);
    }

    @Test
    public void testClone() {
        VDBMetaData vdb = TestVDBUtility.buildVDB();
        vdb.setXmlDeployment(true);
        VDBMetaData clone = vdb.clone();
        assertTrue(clone.isXmlDeployment());
        assertEquals(1, vdb.getVDBImports().size());
        assertNotSame(clone.getModelMetaDatas(), vdb.getModelMetaDatas());
        //assertNotSame(clone.getDataPolicyMap(), vdb.getDataPolicyMap());
    }

    public static ModelNode describe(ModelNode node, AttributeDefinition[] attributes) {
        for (AttributeDefinition ad : attributes) {
            ad.addResourceAttributeDescription(node, NonResolvingResourceDescriptionResolver.INSTANCE,
                    null, NonResolvingResourceDescriptionResolver.INSTANCE.getResourceBundle(null));
        }
        return node;
    }
}
