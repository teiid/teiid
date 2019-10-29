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
package org.teiid.jboss.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.stream.XMLStreamException;

import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.TestCompositeVDB;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.TestDDLParser;
import org.teiid.query.unittest.RealMetadataFactory;

import io.swagger.annotations.ApiOperation;

@SuppressWarnings("nls")
public class TestRestWebArchiveBuilder {

    private static class ASMUtilities {

        public static Class<?> defineClass(String name, byte[] bytes) {
            return new TestClassLoader(TestClassLoader.class.getClassLoader()).defineClassForName(name, bytes);
        }

        private static  class TestClassLoader extends ClassLoader {

            public TestClassLoader(ClassLoader classLoader) {
                super(classLoader);
            }

            public Class<?> defineClassForName(String name, byte[] data) {
                return this.defineClass(name, data, 0, data.length);
            }

        }
    }

    private VDBMetaData getTestVDBMetaData() throws FileNotFoundException, XMLStreamException {
        VDBMetaData vdb = VDBMetadataParser.unmarshall(new FileInputStream(UnitTestUtil.getTestDataFile("sample-vdb.xml")));
        MetadataStore ms = new MetadataStore();
        for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
            MetadataFactory mf = TestDDLParser.helpParse(model.getSchemaText(), model.getName());
            ms.addSchema(mf.getSchema());
        }

        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(ms, "Rest");
        vdb.addAttachment(QueryMetadataInterface.class, metadata);
        vdb.addAttachment(TransformationMetadata.class, metadata);
        vdb.addAttachment(MetadataStore.class, ms);
        return vdb;
    }

    @Test
    public void testBuildArchive() throws Exception {

        VDBMetaData vdb = getTestVDBMetaData();
        RestASMBasedWebArchiveBuilder builder = new RestASMBasedWebArchiveBuilder();
        byte[] contents = builder.getContent(vdb, "vdb");

        ArrayList<String> files = new ArrayList<String>();
        files.add("WEB-INF/web.xml");
        files.add("WEB-INF/jboss-web.xml");
        files.add("WEB-INF/classes/org/teiid/jboss/rest/View.class");
        files.add("WEB-INF/classes/org/teiid/jboss/rest/TeiidRestApplication.class");
        files.add("WEB-INF/classes/org/teiid/jboss/rest/Bootstrap.class");
        files.add("META-INF/MANIFEST.MF");

        files.add("api.html");
        files.add("images/teiid_logo_450px.png");

        files.add("swagger/swagger-ui.js");

        files.add("swagger/css/print.css");
        files.add("swagger/css/reset.css");
        files.add("swagger/css/screen.css");
        files.add("swagger/css/style.css");
        files.add("swagger/css/typography.css");

        files.add("swagger/images/favicon-16x16.png");
        files.add("swagger/images/favicon-32x32.png");

        files.add("swagger/lang/en.js");
        files.add("swagger/lang/es.js");
        files.add("swagger/lang/pt.js");
        files.add("swagger/lang/ru.js");
        files.add("swagger/lang/translator.js");

        files.add("swagger/lib/backbone-min.js");
        files.add("swagger/lib/handlebars-2.0.0.js");
        files.add("swagger/lib/highlight.7.3.pack.js");
        files.add("swagger/lib/jquery-1.8.0.min.js");
        files.add("swagger/lib/jquery.ba-bbq.min.js");
        files.add("swagger/lib/jquery.slideto.min.js");
        files.add("swagger/lib/jquery.wiggle.min.js");
        files.add("swagger/lib/marked.js");
        files.add("swagger/lib/swagger-oauth.js");
        files.add("swagger/lib/underscore-min.js");
        files.add("swagger/lib/underscore-min.map");

        ZipInputStream zipIn = new ZipInputStream(new ByteArrayInputStream(contents));
        ZipEntry ze;
        while ((ze = zipIn.getNextEntry()) != null) {
            assertTrue(files.contains(ze.getName()));
            zipIn.closeEntry();
        }
    }

    @Test
    public void testBuildArchiveSwagger() throws Exception {

        VDBMetaData vdb = getTestVDBMetaData();
        RestASMBasedWebArchiveBuilder builder = new RestASMBasedWebArchiveBuilder();

        MetadataStore metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
        for (ModelMetaData model:vdb.getModelMetaDatas().values()) {
            Schema schema = metadataStore.getSchema(model.getName());
            byte[] viewContents = builder.getViewClass(vdb.getName(), vdb.getVersion(), model.getName(), schema, false);
            if (viewContents != null){
                Class<?> cls = ASMUtilities.defineClass("org.teiid.jboss.rest.View", viewContents);

                Set<Annotation> annotationSet = new HashSet<Annotation>();
                for(Annotation annotation : cls.getAnnotations()) {
                    annotationSet.add(annotation);
                }
                assertEquals(2, annotationSet.size());

                for(Method m : cls.getMethods()){
                    if(m.getName().equals("g1Tableapplication_xml")){
                        ApiOperation annotation = m.getAnnotation(ApiOperation.class);
                        assertEquals("g1Table", annotation.value());

                    } else if(m.getName().equals("sqlQueryxml")){
                        ApiOperation annotation = m.getAnnotation(ApiOperation.class);
                        assertEquals("xml", annotation.value());

                    } else if(m.getName().equals("sqlQueryjson")){
                        ApiOperation annotation = m.getAnnotation(ApiOperation.class);
                        assertEquals("json", annotation.value());

                    }
                }
            }
        }
    }

    @Test
    public void testBootstrapServletClass() throws InstantiationException, IllegalAccessException {

        RestASMBasedWebArchiveBuilder builder = new RestASMBasedWebArchiveBuilder();
        byte[] bytes = builder.getBootstrapServletClass("vdbName", "", "version", new String[]{"http"}, "baseUrl", "packages", true);
        Class<?> cls = ASMUtilities.defineClass("org.teiid.jboss.rest.Bootstrap", bytes);
        Object obj = cls.newInstance();
        assertEquals(cls, obj.getClass());
    }


    @Test
    public void testOtherModels() throws Exception {

        MetadataStore ms = new MetadataStore();

        CompositeVDB vdb = TestCompositeVDB.createCompositeVDB(ms, "x");
        vdb.getVDB().addProperty("{http://teiid.org/rest}auto-generate", "true");
        ModelMetaData model = new ModelMetaData();
        model.setName("other");
        model.setModelType(Type.OTHER);
        vdb.getVDB().addModel(model);

        RestASMBasedWebArchiveBuilder builder = new RestASMBasedWebArchiveBuilder();
        builder.getContent(vdb.getVDB(), "vdb");
    }
}
