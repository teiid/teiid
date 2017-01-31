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

import java.io.*;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Iterator;

import javax.xml.stream.XMLStreamException;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.Admin.ExportFormat;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheFactory;
import org.teiid.core.util.LRUCache;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.logging.LogManager;
import org.teiid.metadata.*;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.JBossLogger;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class ConvertVDB {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length < 1) {
            System.out.println("usage: CovertVDB /path/to/file.vdb");
            System.exit(0);
        }

        File f = new File(args[0]);
        if (!f.exists()) {
            System.out.println("vdb file does not exist");
        }
        
        if (f.getName().toLowerCase().endsWith(".vdb") || f.getName().toLowerCase().endsWith(".xml")) {
            System.out.println(convert(f));
        } else {
            System.out.println("Unknown file type supplied, only .VDB, .XML based VDBs are supported");
        }
    }

    public static String convert(File f)
            throws VirtualDatabaseException, ConnectorManagerException, TranslatorException, IOException,
            URISyntaxException, MalformedURLException, AdminException, Exception, FileNotFoundException {
        
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
                return new MockCache<>(name, 10);
            }
            @Override
            public void destroy() {
            }
        });
        
        MyServer es = new MyServer();
        
        LogManager.setLogListener(new JBossLogger() {
            @Override
            public boolean isEnabled(String context, int level) {
                return false;
            }
        });        
        
        es.start(ec);
        try {
            if (f.getName().toLowerCase().endsWith(".vdb")) {
                es.deployVDBZip(f.toURI().toURL());
                Admin admin = es.getAdmin();
                VDB vdb = admin.getVDBs().iterator().next();
                String format = System.getProperty("format", "DDL");
                return admin.getSchema(vdb.getName(), vdb.getVersion(), null, null, null, ExportFormat.valueOf(format));
            } else if (f.getName().toLowerCase().endsWith(".xml")) {
                return es.convertVDB(new FileInputStream(f));
            } 
        } finally {
            es.stop();
        }
        return null;
    }
    
    private static class MyServer extends EmbeddedServer {
        @Override
        public ExecutionFactory<Object, Object> getExecutionFactory(String name) throws ConnectorManagerException {
            return new ExecutionFactory<Object, Object>() {

                @Override
                public boolean isSourceRequired() {
                    return false;
                }

                @Override
                public boolean isSourceRequiredForMetadata() {
                    return false;
                }
            };
        };

        String convertVDB(InputStream is) throws Exception{
            byte[] bytes = ObjectConverterUtil.convertToByteArray(is);
            VDBMetaData metadata = null;
            try {
                metadata = VDBMetadataParser.unmarshell(new ByteArrayInputStream(bytes));
            } catch (XMLStreamException e) {
                throw new VirtualDatabaseException(e);
            }
            metadata.setXmlDeployment(true);
            
            MetadataStore metadataStore = new MetadataStore();
            for (ModelMetaData m:metadata.getModelMetaDatas().values()) {
                Schema schema = new Schema();
                schema.setName(m.getName());
                Table table = new Table();
                table.setTableType(Table.Type.Table);
                table.setName("__temp__");
                Column column = new Column();
                column.setName("x");
                Datatype datatype = SystemMetadata.getInstance().getDataTypes().get(0);
                column.setDatatype(datatype);
                table.addColumn(column);
                column.setParent(table);

                schema.addTable(table);
                metadataStore.addSchema(schema);
            }
            
            Database db = DatabaseUtil.convert(metadata, metadataStore);
            String contents = DDLStringVisitor.getDDLString(db);
            String replace = "";
            String find = "CREATE FOREIGN TABLE \"__temp__\" (\n" + 
                    "\tx xml OPTIONS (CASE_SENSITIVE FALSE)\n" + 
                    ");\n";
            contents = contents.replace(find, replace);

            for (ModelMetaData m:metadata.getModelMetaDatas().values()) {
                find = "SET SCHEMA "+m.getName()+";\n";
                if (m.isSource()) {
                    String sourceName = m.getSourceNames().get(0);
                    String schemaName = m.getPropertiesMap().get("importer.schemaPattern");
                    if (schemaName == null) {
                        schemaName = "public";
                    }
                    
                    if (m.getSourceMetadataType().isEmpty()) {
                        // nothing defined; so this is NATIVE only
                       replace = replaceNative(m, sourceName, schemaName);                   
                    } else {
                        // may one or more defined
                        for (int i = 0; i < m.getSourceMetadataType().size(); i++) {
                            String type =  m.getSourceMetadataType().get(i);
                            if (type.equalsIgnoreCase("NATIVE")) {
                                replace += replaceNative(m, sourceName, schemaName);
                            } else if (type.equalsIgnoreCase("DDL")){
                                replace = m.getSourceMetadataText().get(0);
                            }
                        }
                    }
                } else {
                    replace = m.getSourceMetadataText().get(0);
                }
                contents = contents.replace(find, find+replace);
            }
            return contents;
        }

        private String replaceNative(ModelMetaData m, String sourceName, String schemaName) {
            String replace;
            replace = "IMPORT FOREIGN SCHEMA "+schemaName+" FROM SERVER "+sourceName+" INTO "+m.getName()+" OPTIONS (\n";
               Iterator<String> it = m.getPropertiesMap().keySet().iterator();
               while (it.hasNext()) {
                   String key = it.next();
                   replace += ("\t"+key +" '"+m.getPropertiesMap().get(key)+"'");
                   if (it.hasNext()) {
                       replace += ",\n";
                   }
               }
               replace += ");\n";
            return replace;
        }
    };
    
    private static class MockCache<K, V> extends LRUCache<K, V> implements Cache<K, V> {
        
        private String name;
        
        public MockCache(String cacheName, int maxSize) {
            super(maxSize<0?Integer.MAX_VALUE:maxSize);
            this.name = cacheName;
        }
        
        @Override
        public V put(K key, V value, Long ttl) {
            return put(key, value);
        }
    
        @Override
        public String getName() {
            return this.name;
        }
    
        @Override
        public boolean isTransactional() {
            return false;
        }
    }    
}
