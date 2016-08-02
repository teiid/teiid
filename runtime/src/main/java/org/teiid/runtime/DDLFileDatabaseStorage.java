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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.teiid.metadata.Database;
import org.teiid.metadata.MetadataException;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.DatabaseStorage;
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.parser.QueryParser;


public class DDLFileDatabaseStorage implements DatabaseStorage {
    private DatabaseStore store;
    private Properties properties = new Properties();
    private FilePersistence persistence = new FilePersistence(); 
    
    @Override
    public void load() {        
        File[] files = getFiles();
        for (File f : files) {
            try {
                getStore().startEditing();
                parseDDL(f);
            } finally {
                getStore().stopEditing();
            }
        }
    }
    
    public void stop() {
    }    

    private static class DDLFiles implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            name = name.toLowerCase();
            return name.endsWith("-vdb.ddl");
        }
    }
    
    private File[] getFiles() {
        String location = getLocation();
        
        File f = new File(location);
        if (!f.exists()) {
            throw new MetadataException(RuntimePlugin.Event.TEIID40149,
                    RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40149, location));            
        }
        if (f.isDirectory()) {
            if (f.listFiles(new DDLFiles()).length == 0) {
                try {
                    FileWriter fw = new FileWriter(new File(f, "ADMIN.1-vdb.ddl"));
                    fw.write("CREATE DATABASE ADMIN VERSION '1';");
                    fw.close();
                } catch (IOException e) {
                    throw new MetadataException(RuntimePlugin.Event.TEIID40149,
                            RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40149, location));
                }
            }
            return f.listFiles(new DDLFiles());
        }
        return new File[] {f};
    }

    private String getLocation() {
        String location = this.properties.getProperty("location");
        if (location == null) {
            location = System.getProperty("jboss.server.config.dir") +"/"+"teiid-repository/";
            File f = new File(location);
            if (!f.exists()) {
                f.mkdirs();
            }
        }
        return location;
    }
    
    private void parseDDL(File f) {        
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(f));
            QueryParser parser = new QueryParser();
            parser.parseDDL(getStore(), reader);
        } catch (IOException e) {
            throw new MetadataException(RuntimePlugin.Event.TEIID40149,
                    RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40149, f.getName()));
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    //ignore.
                }
            }
        }
    }
    
    @Override
    public void setStore(DatabaseStore store) {
        this.store = store;
    }
    
    @Override
    public DatabaseStore getStore() {
        return this.store;
    }

    @Override
    public void setProperties(String str) {
        if (this.properties != null) {
            StringTokenizer st = new StringTokenizer(str, ",");
            while(st.hasMoreTokens()) {
                String prop = st.nextToken();
                int index = prop.indexOf('=');
                if (index != -1) {
                    this.properties.setProperty(prop.substring(0, index), prop.substring(index+1));
                }
            }
        }
    }

    class FilePersistence implements DatabaseStorage.PersistenceProxy {
        @Override
        public void save(Database database) {
            if (database == null) {
                return;
            }
            File f = getFile(database);
            try {
                FileWriter fw = new FileWriter(f);
                String ddl = DDLStringVisitor.getDDLString(database);
                fw.write(ddl);
                fw.close();
            } catch (IOException e) {
                throw new MetadataException(RuntimePlugin.Event.TEIID40149,
                        RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40149, f.getName()));            
            }
        }

        private File getFile(Database database) {
            File parent = new File(getLocation());
            File f = null;
            if (parent.isDirectory()) {
                f = new File(parent, database.getName()+"."+database.getVersion()+"-vdb.ddl");    
            } else {
                f = parent;
            }
            return f;
        }

        @Override
        public void drop(Database database) {
            if (database == null) {
                return;
            }
            File f = getFile(database);
            if (f.exists()) {
                f.delete();
            }
        }
    }

    @Override
    public void startRecording(boolean save) {
        if (save) {
            this.store.register(this.persistence);
        }
    }

    @Override
    public void stopRecording() {
        this.store.register(null);
    }
}
