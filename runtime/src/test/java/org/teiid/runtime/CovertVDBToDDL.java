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
package org.teiid.runtime;

import java.io.File;

import org.mockito.Mockito;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.VDB;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.translator.ExecutionFactory;

public class CovertVDBToDDL {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length < 1) {
            System.out.println("usage: CovertVDBToDDL /path/to/file.vdb");
            System.exit(0);
        }
        
        EmbeddedServer es = new EmbeddedServer() {
            @Override
            public ExecutionFactory<Object, Object> getExecutionFactory(String name) throws ConnectorManagerException {
                return Mockito.mock(ExecutionFactory.class);
            }
        };
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        
        File f = new File(args[0]);
        if (!f.exists()) {
            System.out.println("vdb file does not exist");
        }
        es.start(ec);
        
        es.deployVDBZip(f.toURI().toURL());
        Admin admin = es.getAdmin();
        for (VDB vdb:admin.getVDBs()) {
            String ddl = admin.getSchema(vdb.getName(), vdb.getVersion(), null, null, null);
            System.out.println(ddl);
        }
        es.stop();
    }
}
