/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.jboss.rest;

import static org.junit.Assert.*;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Test;
import org.objectweb.asm.ClassWriter;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.TestDDLParser;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestRestWebArchiveBuilder {

	@Test
	public void testBuildArchive() throws Exception {
		VDBMetaData vdb = VDBMetadataParser.unmarshell(new FileInputStream(UnitTestUtil.getTestDataFile("sample-vdb.xml")));
		MetadataStore ms = new MetadataStore();
		for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
			MetadataFactory mf = TestDDLParser.helpParse(model.getSchemaText(), model.getName());
			ms.addSchema(mf.getSchema());
		}
		
		TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(ms, "Rest");
		vdb.addAttchment(QueryMetadataInterface.class, metadata);
		vdb.addAttchment(TransformationMetadata.class, metadata);
		vdb.addAttchment(MetadataStore.class, ms);
		
		RestASMBasedWebArchiveBuilder builder = new RestASMBasedWebArchiveBuilder();
		byte[] contents = builder.createRestArchive(vdb);
		
		ArrayList<String> files = new ArrayList<String>();
		files.add("WEB-INF/web.xml");
		files.add("WEB-INF/jboss-web.xml");
		files.add("WEB-INF/classes/org/teiid/jboss/rest/View.class");
		files.add("WEB-INF/classes/org/teiid/jboss/rest/TeiidRestApplication.class");
		files.add("META-INF/MANIFEST.MF");
		
		files.add("api-doc.html");
		files.add("o2c.html");
		files.add("swagger-ui.min.js");
		files.add("swagger-ui.js");
		
		files.add("css/print.css");
		files.add("css/reset.css");
        files.add("css/screen.css");
        files.add("css/style.css");
        files.add("css/typography.css");
        
        files.add("fonts/droid-sans-v6-latin-700.eot");
        files.add("fonts/droid-sans-v6-latin-700.svg");
        files.add("fonts/droid-sans-v6-latin-700.ttf");
        files.add("fonts/droid-sans-v6-latin-700.woff");
        files.add("fonts/droid-sans-v6-latin-700.woff2");
        files.add("fonts/droid-sans-v6-latin-regular.eot");
        files.add("fonts/droid-sans-v6-latin-regular.svg");
        files.add("fonts/droid-sans-v6-latin-regular.ttf");
        files.add("fonts/droid-sans-v6-latin-regular.woff");
        files.add("fonts/droid-sans-v6-latin-regular.woff2");
        
        files.add("images/explorer_icons.png");
        files.add("images/favicon-16x16.png");
        files.add("images/favicon-32x32.png");
        files.add("images/favicon.ico");
        files.add("images/logo_small.png");
        files.add("images/pet_store_api.png");
        files.add("images/wordnik_api.png");
        
        files.add("lang/en.js");
        files.add("lang/es.js");
        files.add("lang/pt.js");
        files.add("lang/ru.js");
        files.add("lang/translator.js");
        
        files.add("lib/backbone-min.js");
        files.add("lib/handlebars-2.0.0.js");
        files.add("lib/highlight.7.3.pack.js");
        files.add("lib/jquery-1.8.0.min.js");
        files.add("lib/jquery.ba-bbq.min.js");
        files.add("lib/jquery.slideto.min.js");
        files.add("lib/jquery.wiggle.min.js");
        files.add("lib/marked.js");
        files.add("lib/swagger-oauth.js");
        files.add("lib/underscore-min.js");
        files.add("lib/underscore-min.map");
		
		ZipInputStream zipIn = new ZipInputStream(new ByteArrayInputStream(contents));
		ZipEntry ze;
		while ((ze = zipIn.getNextEntry()) != null) {
			assertTrue(files.contains(ze.getName()));
			zipIn.closeEntry();
		}
	}
	
	@Test
    public void testBuildArchiveSwagger() throws Exception {
	    VDBMetaData vdb = VDBMetadataParser.unmarshell(new FileInputStream(UnitTestUtil.getTestDataFile("sample-vdb.xml")));
        MetadataStore ms = new MetadataStore();
        for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
            MetadataFactory mf = TestDDLParser.helpParse(model.getSchemaText(), model.getName());
            ms.addSchema(mf.getSchema());
        }
        
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(ms, "Rest");
        vdb.addAttchment(QueryMetadataInterface.class, metadata);
        vdb.addAttchment(TransformationMetadata.class, metadata);
        vdb.addAttchment(MetadataStore.class, ms);
        
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
                        Parameter[] params = m.getParameters();
                        Parameter param = params[0];
                        ApiParam a = param.getAnnotation(ApiParam.class);
                        assertEquals("p1", a.value());
                    } else if(m.getName().equals("sqlQueryxml")){
                        ApiOperation annotation = m.getAnnotation(ApiOperation.class);
                        assertEquals("xml", annotation.value());
                        Parameter[] params = m.getParameters();
                        Parameter param = params[0];
                        ApiParam a = param.getAnnotation(ApiParam.class);
                        assertEquals("xml", a.value());
                    } else if(m.getName().equals("sqlQueryjson")){
                        ApiOperation annotation = m.getAnnotation(ApiOperation.class);
                        assertEquals("json", annotation.value());
                        Parameter[] params = m.getParameters();
                        Parameter param = params[0];
                        ApiParam a = param.getAnnotation(ApiParam.class);
                        assertEquals("json", a.value());
                    }
                }
                                
            }
        }
        
	}
	
	public static class ASMUtilities {
	    
	    public static Class<?> defineClass(String name, byte[] bytes) {
	        return new TestClassLoader(TestClassLoader.class.getClassLoader()).defineClassForName(name, bytes);
	    }

	    public static Class<?> defineClass(String name, ClassWriter writer) {
	        return defineClass(name, writer.toByteArray());
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

}
