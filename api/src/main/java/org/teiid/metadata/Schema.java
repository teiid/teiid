/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.teiid.connector.DataPlugin;

public class Schema extends AbstractMetadataRecord {

    private enum ChildType {
        server,
        function,
        procedure,
        table;
    }

    private static final long serialVersionUID = -5113742472848113008L;

    private boolean physical = true;
    private String primaryMetamodelUri = "http://www.metamatrix.com/metamodels/Relational"; //$NON-NLS-1$

    private NavigableMap<String, Table> tables = new TreeMap<String, Table>(String.CASE_INSENSITIVE_ORDER);
    private NavigableMap<String, Procedure> procedures = new TreeMap<String, Procedure>(String.CASE_INSENSITIVE_ORDER);
    private NavigableMap<String, FunctionMethod> functions = new TreeMap<String, FunctionMethod>(String.CASE_INSENSITIVE_ORDER);
    private NavigableMap<String, Server> servers = new TreeMap<String, Server>(String.CASE_INSENSITIVE_ORDER);
    private List<AbstractMetadataRecord> resolvingOrder = new ArrayList<AbstractMetadataRecord>();

    public void addTable(Table table) {
        table.setParent(this);
        if (this.tables.put(table.getName(), table) != null) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60013, DataPlugin.Util.gs(DataPlugin.Event.TEIID60013, table.getName()));
        }
        resolvingOrder.add(table);
    }

    public Table removeTable(String tableName) {
        Table previous = this.tables.remove(tableName);
        if (previous != null){
            resolvingOrder.remove(previous);
        }
        return previous;
    }

    public void addProcedure(Procedure procedure) {
        procedure.setParent(this);
        if (this.procedures.put(procedure.getName(), procedure) != null) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60014, DataPlugin.Util.gs(DataPlugin.Event.TEIID60014, procedure.getName()));
        }
        resolvingOrder.add(procedure);
    }

    public Procedure removeProcedure(String procedureName) {
        Procedure previous = this.procedures.remove(procedureName);
        if (previous != null){
            resolvingOrder.remove(previous);
        }
        return previous;
    }

    public void addFunction(FunctionMethod function) {
        function.setParent(this);
        // hash based check, which allows overloaded functions
        HashSet<FunctionMethod> funcs = new HashSet<FunctionMethod>();
        for (FunctionMethod fm : getFunctions().values()) {
            funcs.add(fm);
        }
        if (funcs.contains(function)) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60015,
                    DataPlugin.Util.gs(DataPlugin.Event.TEIID60015, function.getName()));
        }

        //TODO: ensure that all uuids are unique
        if (this.functions.put(function.getUUID(), function) != null) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60015, DataPlugin.Util.gs(DataPlugin.Event.TEIID60015, function.getUUID()));
        }
        resolvingOrder.add(function);
    }

    public List<FunctionMethod> removeFunctions(String functionName) {
        ArrayList<FunctionMethod> funcs = new ArrayList<FunctionMethod>();
        for (FunctionMethod fm : this.functions.values()){
            if (fm.getName().equalsIgnoreCase(functionName)){
                funcs.add(fm);
            }
        }

        for (FunctionMethod func:funcs) {
            this.functions.remove(func.getUUID());
        }
        return funcs;
    }

    /**
     * Get the tables defined in this schema
     * @return
     */
    public NavigableMap<String, Table> getTables() {
        return tables;
    }

    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    /**
     * Get the procedures defined in this schema
     * @return
     */
    public NavigableMap<String, Procedure> getProcedures() {
        return procedures;
    }

    public Procedure getProcedure(String procName) {
        return procedures.get(procName);
    }

    /**
     * Get the functions defined in this schema in a map of uuid to {@link FunctionMethod}
     * @return
     */
    public NavigableMap<String, FunctionMethod> getFunctions() {
        return functions;
    }

    /**
     * Get a function by uid
     * @return
     */
    public FunctionMethod getFunction(String uid) {
        return functions.get(uid);
    }

    public String getPrimaryMetamodelUri() {
        return primaryMetamodelUri;
    }

    public boolean isPhysical() {
        return physical;
    }

    /**
     * @param string
     */
    public void setPrimaryMetamodelUri(String string) {
        primaryMetamodelUri = string;
    }

    public void setPhysical(boolean physical) {
        this.physical = physical;
    }

    /**
     * 7.1 schemas did not have functions
     */
    private void readObject(java.io.ObjectInputStream in)
    throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (this.functions == null) {
            this.functions = new TreeMap<String, FunctionMethod>(String.CASE_INSENSITIVE_ORDER);
        }
        if (this.resolvingOrder == null) {
            this.resolvingOrder = new ArrayList<AbstractMetadataRecord>();
            this.resolvingOrder.addAll(this.tables.values());
            this.resolvingOrder.addAll(this.procedures.values());
            this.resolvingOrder.addAll(this.functions.values());
        }
    }

    public List<AbstractMetadataRecord> getResolvingOrder() {
        return resolvingOrder;
    }

    public void addServer(Server server) {
        this.servers.put(server.getName(), server);
    }

    public Server getServer(String serverName) {
        return this.servers.get(serverName);
    }

    public List<Server> getServers(){
        return new ArrayList<Server>(this.servers.values());
    }

    public boolean isVisible() {
        String visible = getProperty("VISIBLE", false); //$NON-NLS-1$
        return visible == null || Boolean.valueOf(visible);
    }

    public void setVisible(boolean visible) {
        setProperty("VISIBLE", String.valueOf(visible)); //$NON-NLS-1$
    }

    /**
     * Return the object type name for a given child type class, or null if the class
     * is not a child of a Schema
     * @param child
     * @return
     */
    public static String getChildType(Class<? extends AbstractMetadataRecord> child) {
        if (child == null) {
            return null;
        }
        if (child == FunctionMethod.class) {
            return ChildType.function.name();
        }
        try {
            return ChildType.valueOf(child.getSimpleName().toLowerCase()).name();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * @return the literal value schema
     */
    public static String getTypeName() {
        return "schema"; //$NON-NLS-1$
    }

    /**
     * Get the child of the given type
     * @param type
     * @param id can be either the name or the uid depending on type
     * @return
     */
    public AbstractMetadataRecord getChild(String type, String id) {
        switch (ChildType.valueOf(type)) {
        case function:
            return getFunction(id);
        case procedure:
            return getProcedure(id);
        case server:
            return getServer(id);
        case table:
            return getTable(id);
        }
        return null;
    }
}
