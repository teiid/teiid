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

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Properties;

import org.junit.Test;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.translator.BaseDelegatingExecutionFactory;
import org.teiid.translator.ExecutionFactory;

@SuppressWarnings("nls")
public class TestVDBService {

    public static class SampleExecutionFactory extends BaseDelegatingExecutionFactory<Void, Void> {

    }

    @Test(expected=ConnectorManagerException.class) public void testMissingDelegate() throws ConnectorManagerException {
        TranslatorRepository repo = new TranslatorRepository();
        VDBTranslatorMetaData tmd = new VDBTranslatorMetaData();
        Properties props = new Properties();
        props.put("delegateName", "y");
        tmd.setProperties(props);
        tmd.setExecutionFactoryClass(SampleExecutionFactory.class);
        repo.addTranslatorMetadata("x", tmd);
        TranslatorUtil.getExecutionFactory("x", repo, repo,
                new VDBMetaData(), new IdentityHashMap<Translator, ExecutionFactory<Object, Object>>(), new HashSet<String>());
    }

    @Test public void testAddSupportedFunctions() throws ConnectorManagerException {
        TranslatorRepository repo = new TranslatorRepository();
        VDBTranslatorMetaData tmd = new VDBTranslatorMetaData();
        Properties props = new Properties();
        props.put("delegateName", "y");
        props.put("addSupportedFunctions", "a,b,c");
        tmd.setProperties(props);
        tmd.setExecutionFactoryClass(SampleExecutionFactory.class);
        repo.addTranslatorMetadata("x", tmd);

        VDBTranslatorMetaData tmd1 = new VDBTranslatorMetaData();
        repo.addTranslatorMetadata("y", tmd1);
        IdentityHashMap<Translator, ExecutionFactory<Object, Object>> map = new IdentityHashMap<Translator, ExecutionFactory<Object, Object>>();
        map.put(tmd1, new ExecutionFactory<Object, Object>());

        ExecutionFactory ef = TranslatorUtil.getExecutionFactory("x", repo, repo,
                new VDBMetaData(), map, new HashSet<String>());

        assertEquals(3, ef.getSupportedFunctions().size());
    }

}
