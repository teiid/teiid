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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
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
		
		ZipInputStream zipIn = new ZipInputStream(new ByteArrayInputStream(contents));
		ZipEntry ze;
		while ((ze = zipIn.getNextEntry()) != null) {
			assertTrue(files.contains(ze.getName()));
			zipIn.closeEntry();
		}
	}

}
