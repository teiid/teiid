/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.metadata.runtime;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.metamatrix.modeler.core.index.IndexSelector;
import com.metamatrix.modeler.internal.core.index.CompositeIndexSelector;
import com.metamatrix.modeler.internal.core.index.RuntimeIndexSelector;
import com.metamatrix.modeler.transformation.metadata.ServerMetadataFactory;
import com.metamatrix.query.metadata.QueryMetadataInterface;

public class VDBMetadataFactory {
	
	public static QueryMetadataInterface getVDBMetadata(String vdbFile) {
        IndexSelector selector = new RuntimeIndexSelector(vdbFile);
        return ServerMetadataFactory.getInstance().createCachingServerMetadata(selector); 
    }
	
	public static QueryMetadataInterface getVDBMetadata(URL vdbURL) throws IOException {
        IndexSelector selector = new RuntimeIndexSelector(vdbURL);
        return ServerMetadataFactory.getInstance().createCachingServerMetadata(selector); 
    }	
	
	public static QueryMetadataInterface getVDBMetadata(String[] vdbFile) {
		
        List selectors = new ArrayList();
        for (int i = 0; i < vdbFile.length; i++){
	        selectors.add(new RuntimeIndexSelector(vdbFile[i]));        
        }
        
        IndexSelector composite = new CompositeIndexSelector(selectors);
        return ServerMetadataFactory.getInstance().createCachingServerMetadata(composite);
    }	
}
