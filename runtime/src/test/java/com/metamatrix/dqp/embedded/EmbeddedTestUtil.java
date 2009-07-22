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

package com.metamatrix.dqp.embedded;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.embedded.services.EmbeddedConfigurationService;

public class EmbeddedTestUtil {
	
	public static Properties getProperties() throws IOException {
		Properties p = getProperties(UnitTestUtil.getTestScratchPath()+"/dqp/dqp.properties"); //$NON-NLS-1$
        p.setProperty(DQPEmbeddedProperties.DQP_WORKDIR, p.getProperty(DQPEmbeddedProperties.TEIID_HOME)+"/work"); //$NON-NLS-1$
        p.setProperty(DQPEmbeddedProperties.DQP_DEPLOYDIR, p.getProperty(DQPEmbeddedProperties.TEIID_HOME)+"/deploy");  //$NON-NLS-1$
        
        new File(p.getProperty(DQPEmbeddedProperties.TEIID_HOME)+"/work").mkdirs(); //$NON-NLS-1$
        new File(p.getProperty(DQPEmbeddedProperties.TEIID_HOME)+"/deploy").mkdirs(); //$NON-NLS-1$
        return p;
	}
	
    public static Properties getProperties(String file) throws IOException {
        Properties props = PropertiesUtils.load(file);        
        props.put(EmbeddedConfigurationService.PROPERTIES_URL, URLHelper.buildURL(file));
        return props;
    }
    
    public static void createTestDirectory() throws Exception {
    	File scratchDQP = UnitTestUtil.getTestScratchFile("dqp"); //$NON-NLS-1$
    	FileUtils.removeDirectoryAndChildren(scratchDQP);
    	FileUtils.copyDirectoriesRecursively(UnitTestUtil.getTestDataFile("dqp"), new File(UnitTestUtil.getTestScratchPath())); //$NON-NLS-1$
    }

}
