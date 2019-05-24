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
package org.teiid.eclipselink.platform;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.file.JavaVirtualFile;
import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.resource.api.ConnectionFactory;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.file.FileExecutionFactory;

@SuppressWarnings("nls")
public class TestTeiidPlatform {

    static EmbeddedServer server;
    static EntityManagerFactory factory;

    @BeforeClass
    public static void init() throws Exception {

        server = new EmbeddedServer();
        FileExecutionFactory executionFactory = new FileExecutionFactory();
        server.addTranslator("file", executionFactory);

        server.addConnectionFactory("java:/marketdata-file", new ConnectionFactory<VirtualFileConnection>() {
            @Override
            public VirtualFileConnection getConnection() throws Exception {
                VirtualFileConnection result = Mockito.mock(VirtualFileConnection.class);
                JavaVirtualFile javaVirtualFile = new JavaVirtualFile(UnitTestUtil.getTestDataFile("file/marketdata.csv"));
                Mockito.stub(result.getFiles(Mockito.anyString())).toReturn(new VirtualFile[] {javaVirtualFile});
                return result;
            }
        });

        EmbeddedConfiguration config = new EmbeddedConfiguration();
        server.start(config);
        DriverManager.registerDriver(server.getDriver());

        server.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("vdb"+File.separator+"marketdata-vdb.xml")));

        factory = Persistence.createEntityManagerFactory("org.teiid.eclipselink.test");
    }

    @Test
    public void testInit() throws Exception {
        assertNotNull(factory);
        EntityManager em = factory.createEntityManager();
        assertNotNull(em);
        em.close();
    }

    @Test
    public void testJPQLQuery() {
        EntityManager em = factory.createEntityManager();
        List list = em.createQuery("SELECT m FROM Marketdata m").getResultList();
        assertNotNull(list);
        assertEquals(10, list.size());
        em.close();
    }

    @AfterClass
    public static void destory() {

        factory.close();
        try {
            DriverManager.deregisterDriver(server.getDriver());
        } catch (SQLException e) {
        }
        server.stop();
    }

}
