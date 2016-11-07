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
package org.teiid.translator.object.testdata.trades;
 
import java.io.File;
import java.net.URL;

import org.teiid.cdk.api.TranslationUtility;
import org.teiid.metadata.RuntimeMetadata;

@SuppressWarnings("nls")
public class VDBUtility {

	public static TranslationUtility TRANSLATION_UTILITY = null;

	public static RuntimeMetadata RUNTIME_METADATA = null;
	
	static {
		
		URL urlToFile = VDBUtility.class.getClassLoader().getResource("ObjectProject" + File.separatorChar + "Trade.vdb");
		if (urlToFile == null) {
			throw new RuntimeException("Unable to get URL for file " + "trade.vdb");
		}
//		File f = new File("." +  File.separatorChar + "ObjectProject" + File.separatorChar + "Trade.vdb");
//		System.out.println("TestDataPath " + f.getAbsolutePath());
		try {
			TRANSLATION_UTILITY = new TranslationUtility("Trade.vdb", urlToFile);
					//f.toURI().toURL());
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
		RUNTIME_METADATA = VDBUtility.TRANSLATION_UTILITY.createRuntimeMetadata();
	}
	

}
