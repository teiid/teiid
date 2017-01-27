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
package org.teiid.runtime.util;

import java.io.File;
import java.io.FileInputStream;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.Admin.ExportFormat;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheFactory;
import org.teiid.adminapi.VDB;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.logging.LogManager;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.JBossLogger;
import org.teiid.translator.ExecutionFactory;

public class CovertVDB {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length < 2) {
            System.out.println("usage: CovertVDB (XML|DDL) /path/to/file.vdb");
            System.exit(0);
        }
        LogManager.setLogListener(new JBossLogger() {
            @Override
            public boolean isEnabled(String context, int level) {
                return false;
            }
        });
        EmbeddedServer es = new EmbeddedServer() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public ExecutionFactory<Object, Object> getExecutionFactory(String name) throws ConnectorManagerException {
                return new ExecutionFactory();
            }
        };
        
        LogManager.setLogListener(new JBossLogger() {
            @Override
            public boolean isEnabled(String context, int level) {
                return false;
            }
        });
        
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        ec.setCacheFactory(new CacheFactory() {
            @Override
            public <K, V> Cache<K, V> get(String name) {
                return null;
            }
            @Override
            public void destroy() {
            }
        });
        File f = new File(args[1]);
        if (!f.exists()) {
            System.out.println("vdb file does not exist");
        }
        es.start(ec);
        
        if (f.getName().toLowerCase().endsWith(".vdb")) {
            es.deployVDBZip(f.toURI().toURL());    
        } else if (f.getName().toLowerCase().endsWith(".xml")) {
            es.deployVDB(new FileInputStream(f));
        } else if (f.getName().toLowerCase().endsWith(".ddl")) {
            es.deployVDB(new FileInputStream(f), true);
        } else {
            System.out.println("Unknown file type supplied, only .VDB, .XML, .DDL based VDBs are supported");
            System.exit(-1);            
        }
        
        Admin admin = es.getAdmin();
        for (VDB vdb:admin.getVDBs()) {
            String ddl = admin.getSchema(vdb.getName(), vdb.getVersion(), null, null, null, ExportFormat.valueOf(args[0]));
            System.out.println(ddl);
        }
        es.stop();
    }
}
