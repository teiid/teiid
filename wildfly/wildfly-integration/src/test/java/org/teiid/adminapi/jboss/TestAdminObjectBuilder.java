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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jboss.dmr.ModelNode;
import org.junit.Test;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.translator.ExecutionFactory;

@SuppressWarnings("nls")
public class TestAdminObjectBuilder {

    @Test
    public void testVDB() {

        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("myVDB");
        vdb.setDescription("vdb description");
        vdb.setVersion(1);
        vdb.addProperty("vdb-property", "vdb-value");

        ModelMetaData modelOne = new ModelMetaData();
        modelOne.setName("model-one");
        modelOne.addSourceMapping("s1", "translator", "java:mybinding");
        modelOne.setModelType(Model.Type.PHYSICAL);
        modelOne.addProperty("model-prop", "model-value");
        modelOne.addProperty("model-prop", "model-value-override");
        modelOne.setVisible(false);
        modelOne.addMessage("ERROR", "There is an error in VDB");
        modelOne.setDescription("model description");

        vdb.addModel(modelOne);

        ModelMetaData modelTwo = new ModelMetaData();
        modelTwo.setName("model-two");
        modelTwo.addSourceMapping("s1", "translator", "java:binding-one");
        modelTwo.addSourceMapping("s2", "translator", "java:binding-two");
        modelTwo.setModelType(Model.Type.VIRTUAL);
        modelTwo.addProperty("model-prop", "model-value");

        vdb.addModel(modelTwo);

        VDBTranslatorMetaData t1 = new VDBTranslatorMetaData();
        t1.setName("oracleOverride");
        t1.setType("oracle");
        t1.addProperty("my-property", "my-value");
        List<Translator> list = new ArrayList<Translator>();
        list.add(t1);
        vdb.setOverrideTranslators(list);

        DataPolicyMetadata roleOne = new DataPolicyMetadata();
        roleOne.setName("roleOne");
        roleOne.setDescription("roleOne described");

        PermissionMetaData perm1 = new PermissionMetaData();
        perm1.setResourceName("myTable.T1");
        perm1.setAllowRead(true);
        roleOne.addPermission(perm1);

        PermissionMetaData perm2 = new PermissionMetaData();
        perm2.setResourceName("myTable.T2");
        perm2.setAllowRead(false);
        perm2.setAllowDelete(true);
        roleOne.addPermission(perm2);

        roleOne.setMappedRoleNames(Arrays.asList("ROLE1", "ROLE2"));

        vdb.addDataPolicy(roleOne);

        // convert to managed object and build the VDB out of MO
        ModelNode node = VDBMetadataMapper.INSTANCE.wrap(vdb, new ModelNode());
        vdb = VDBMetadataMapper.INSTANCE.unwrap(node);

        assertEquals("myVDB", vdb.getName());
        assertEquals("vdb description", vdb.getDescription());
        assertEquals("1", vdb.getVersion());
        assertEquals("vdb-value", vdb.getPropertyValue("vdb-property"));

        assertNotNull(vdb.getModel("model-one"));
        assertNotNull(vdb.getModel("model-two"));
        assertNull(vdb.getModel("model-unknown"));

        modelOne = vdb.getModel("model-one");
        assertEquals("model-one", modelOne.getName());
        assertEquals("s1", modelOne.getSourceNames().get(0));
        assertEquals(Model.Type.PHYSICAL, modelOne.getModelType());
        assertEquals("model-value-override", modelOne.getPropertyValue("model-prop"));
        assertFalse(modelOne.isVisible());
        assertEquals("model description", modelOne.getDescription());

        modelTwo = vdb.getModel("model-two");
        assertEquals("model-two", modelTwo.getName());
        assertTrue(modelTwo.getSourceNames().contains("s1"));
        assertTrue(modelTwo.getSourceNames().contains("s2"));
        assertEquals(Model.Type.VIRTUAL, modelTwo.getModelType()); // this is not persisted in the XML
        assertEquals("model-value", modelTwo.getPropertyValue("model-prop"));


        assertTrue(vdb.getValidityErrors().contains("There is an error in VDB"));

        List<Translator> translators = vdb.getOverrideTranslators();
        assertTrue(translators.size() == 1);

        Translator translator = translators.get(0);
        assertEquals("oracleOverride", translator.getName());
        assertEquals("oracle", translator.getType());
        assertEquals("my-value", translator.getPropertyValue("my-property"));

        List<DataPolicy> roles = vdb.getDataPolicies();

        assertTrue(roles.size() == 1);

        DataPolicyMetadata role = vdb.getDataPolicyMap().get("roleOne");
        assertEquals("roleOne described", role.getDescription());
        assertNotNull(role.getMappedRoleNames());
        assertTrue(role.getMappedRoleNames().contains("ROLE1"));
        assertTrue(role.getMappedRoleNames().contains("ROLE2"));

        List<DataPolicy.DataPermission> permissions = role.getPermissions();
        assertEquals(2, permissions.size());

        for (DataPolicy.DataPermission p: permissions) {
            if (p.getResourceName().equalsIgnoreCase("myTable.T1")) {
                assertTrue(p.getAllowRead());
                assertNull(p.getAllowDelete());
            }
            else {
                assertFalse(p.getAllowRead());
                assertTrue(p.getAllowDelete());
            }
        }
    }

    @Test
    public void testTranslator() {
        VDBTranslatorMetaData tm = new VDBTranslatorMetaData();

        tm.setExecutionFactoryClass(ExecutionFactory.class);
        tm.setName("Oracle");
        tm.addProperty("ExtensionTranslationClassName", "org.teiid.translator.jdbc.oracle.OracleSQLTranslator");

        // convert to managed object and build the VDB out of MO
        ModelNode node = VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE.wrap(tm, new ModelNode());
        VDBTranslatorMetaData tm1 = VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE.unwrap(node);

        assertEquals("Oracle", tm1.getName());
        assertEquals(ExecutionFactory.class.getName(), tm1.getPropertyValue(Translator.EXECUTION_FACTORY_CLASS));
        assertEquals("org.teiid.translator.jdbc.oracle.OracleSQLTranslator", tm1.getPropertyValue("ExtensionTranslationClassName"));
    }
}
