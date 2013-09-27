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

package org.teiid.jboss;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Properties;

import org.junit.Test;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
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
		VDBService.getExecutionFactory("x", repo, repo, 
				new VDBMetaData(), new IdentityHashMap<Translator, ExecutionFactory<Object, Object>>(), new HashSet<String>());
	}
	
}
