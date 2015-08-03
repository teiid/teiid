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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestGlobalTemp {
	
	@Test public void testGlobalTempUse() throws Exception {
		TempTableTestHarness harness = new TempTableTestHarness();
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("create global temporary table temp (x serial, s string) options (updatable true);" +
				"", "x", "y");
		HardcodedDataManager dm = new HardcodedDataManager();
		harness.setUp(metadata, dm);
		
		harness.execute("select * from temp", new List<?>[0]);
		harness.execute("insert into temp (s) values ('a')", new List<?>[] {Arrays.asList(1)});
		harness.execute("select * from temp", new List<?>[] {Arrays.asList(1, "a")});
		try {
			harness.execute("drop table temp", new List<?>[0]);
			fail();
		} catch (QueryValidatorException e) {
		}
	}
	
	@Test public void testInsertCreation() throws Exception {
		TempTableTestHarness harness = new TempTableTestHarness();
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("create global temporary table temp (x serial, s string) options (updatable true);" +
				"", "x", "y");
		HardcodedDataManager dm = new HardcodedDataManager();
		harness.setUp(metadata, dm);
		
		harness.execute("insert into temp (s) values ('a')", new List<?>[] {Arrays.asList(1)});
		harness.execute("select * from temp", new List<?>[] {Arrays.asList(1, "a")});
	}
	
	@Test public void testPkInitialUse() throws Exception {
		TempTableTestHarness harness = new TempTableTestHarness();
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("create global temporary table temp (x serial, s string primary key) options (updatable true);" +
				"", "x", "y");
		HardcodedDataManager dm = new HardcodedDataManager();
		harness.setUp(metadata, dm);
		
		harness.execute("select * from temp", new List<?>[] {});
	}
	
}
