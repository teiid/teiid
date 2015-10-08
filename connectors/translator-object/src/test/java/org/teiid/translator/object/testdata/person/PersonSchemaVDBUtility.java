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
package org.teiid.translator.object.testdata.person;
 
import java.io.File;

import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.RuntimeMetadata;

/**
 * This VDBUtility is building the metadata based on the JDG quickstart:  remote-query
 * because this is where the protobuf definitions are defined that are used in unit tests.
 * 
 * Also, the JDG quickstart in Teiid depends upon that quickstart so that user doesn't 
 * have to go generate the proto bin's.
 * 
 * 
 * 
 * @author vanhalbert
 *
 */

@SuppressWarnings("nls")
public class PersonSchemaVDBUtility {

	public static TranslationUtility TRANSLATION_UTILITY = null;

	public static RuntimeMetadata RUNTIME_METADATA = null;
	
	static {
		File f = new File(UnitTestUtil.getTestDataPath() + File.separatorChar + "PersonProject" + File.separatorChar + "Person.vdb");
		System.out.println("TestDataPath " + f.getAbsolutePath());
		try {
			TRANSLATION_UTILITY = new TranslationUtility("Person.vdb", f.toURI().toURL());
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
		RUNTIME_METADATA = PersonSchemaVDBUtility.TRANSLATION_UTILITY.createRuntimeMetadata();
	}
	

}
