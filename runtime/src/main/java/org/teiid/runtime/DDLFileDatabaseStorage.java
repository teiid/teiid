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
    private Properties properties = new Properties();
    private File location;
    
    @Override
    public void load(DatabaseStore store) {        
        File[] files = getFiles();
        for (File f : files) {
            try {
                store.startEditing(false);
                parseDDL(f, store);
            } finally {
                store.stopEditing();
            }
        }
    }
    
    private static class DDLFiles implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            name = name.toLowerCase();
            return name.endsWith("-vdb.ddl"); //$NON-NLS-1$
        }
    }
    
    private File[] getFiles() {
        File f = getLocation();
        
        if (!f.exists()) {
            throw new MetadataException(RuntimePlugin.Event.TEIID40149,
                    RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40149, location));            
        }
        if (f.isDirectory()) {
            if (f.listFiles(new DDLFiles()).length == 0) {
                try {
                    FileWriter fw = new FileWriter(new File(f, "ADMIN.1-vdb.ddl")); //$NON-NLS-1$
                    fw.write("CREATE DATABASE ADMIN VERSION '1';"); //$NON-NLS-1$
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

    private File getLocation() {
        if (location == null) {
            String loc = this.properties.getProperty("location"); //$NON-NLS-1$
            if (loc == null) {
                loc = System.getProperty("jboss.server.config.dir") +"/"+"teiid-repository/"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                location = new File(loc);
                if (!location.exists()) {
                    location.mkdirs();
                }
            } else {
                location = new File(loc);
            }
        }
        return location;
    }
    
    private void parseDDL(File f, DatabaseStore store) {        
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(f));
            QueryParser.getQueryParser().parseDDL(store, reader);
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
    public void setProperties(String str) {
        if (this.properties != null) {
            StringTokenizer st = new StringTokenizer(str, ","); //$NON-NLS-1$
            while(st.hasMoreTokens()) {
                String prop = st.nextToken();
                int index = prop.indexOf('=');
                if (index != -1) {
                    this.properties.setProperty(prop.substring(0, index), prop.substring(index+1));
                }
            }
        }
    }
    
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
    
    @Override
    public void restore(DatabaseStore store, Database database) {
        File f = getFile(database);
        parseDDL(f, store);
    }

    private File getFile(Database database) {
        File parent = getLocation();
        File f = null;
        if (parent.isDirectory()) {
            f = new File(parent, database.getName()+"."+database.getVersion()+"-vdb.ddl"); //$NON-NLS-1$ //$NON-NLS-2$  
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
