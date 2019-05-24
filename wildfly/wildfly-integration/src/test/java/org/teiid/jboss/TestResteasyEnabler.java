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

package org.teiid.jboss;

import java.util.concurrent.Executor;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.util.ExecutorUtils;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.RestWarGenerator;
import org.teiid.deployers.TestCompositeVDB;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.metadata.MetadataStore;

@SuppressWarnings("nls")
public class TestResteasyEnabler {

    @Test public void testOtherModels() throws VirtualDatabaseException {
        RestWarGenerator generator = Mockito.mock(RestWarGenerator.class);
        ResteasyEnabler resteasyEnabler = new ResteasyEnabler(generator) {
            Admin getAdmin() {
                return Mockito.mock(Admin.class);
            }

            Executor getExecutor() {
                return ExecutorUtils.getDirectExecutor();
            }
        };

        MetadataStore ms = new MetadataStore();

        CompositeVDB vdb = TestCompositeVDB.createCompositeVDB(ms, "x");
        vdb.getVDB().addProperty("{http://teiid.org/rest}auto-generate", "true");
        ModelMetaData model = new ModelMetaData();
        model.setName("other");
        model.setModelType(Type.OTHER);
        vdb.getVDB().addModel(model);

        resteasyEnabler.finishedDeployment("x", vdb);
    }

}
