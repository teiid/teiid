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
package org.teiid.dqp.internal.process;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.AbstractMetadataRecord.DataModifiable;
import org.teiid.metadata.AbstractMetadataRecord.Modifiable;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.util.CommandContext;

/**
 * Tracks what views were used and what tables are accessed
 */
public class AccessInfo implements Serializable {

    private static final long serialVersionUID = -2608267960584191359L;

    private transient Set<Object> objectsAccessed;
    private boolean sensitiveToMetadataChanges = true;
    private List<List<String>> externalNames;

    private long creationTime = System.currentTimeMillis();

    private void writeObject(java.io.ObjectOutputStream out)  throws IOException {
        externalNames = initExternalList(externalNames, objectsAccessed);
        out.defaultWriteObject();
    }

    public boolean isSensitiveToMetadataChanges() {
        return sensitiveToMetadataChanges;
    }

    public void setSensitiveToMetadataChanges(boolean sensitiveToMetadataChanges) {
        this.sensitiveToMetadataChanges = sensitiveToMetadataChanges;
    }

    private static List<List<String>> initExternalList(List<List<String>> externalNames, Set<? extends Object> accessed) {
        if (externalNames == null) {
            externalNames = new ArrayList<List<String>>(accessed.size());
            for (Object object : accessed) {
                if (object instanceof AbstractMetadataRecord) {
                    AbstractMetadataRecord t = (AbstractMetadataRecord)object;
                    externalNames.add(Arrays.asList(t.getParent().getName(), t.getName()));
                } else if (object instanceof TempMetadataID) {
                    TempMetadataID t = (TempMetadataID)object;
                    externalNames.add(Arrays.asList(t.getID()));
                }
            }
        }
        return externalNames;
    }

    public void addAccessedObject(Object id) {
        if (this.objectsAccessed == null) {
            this.objectsAccessed = new HashSet<Object>();
        }
        this.objectsAccessed.add(id);
    }

    public Set<Object> getObjectsAccessed() {
        return objectsAccessed;
    }

    public long getCreationTime() {
        return creationTime;
    }

    void populate(CommandContext context, boolean data) {
        Set<Object> objects = null;
        if (data) {
            objects = context.getDataObjects();
        } else {
            objects = context.getPlanningObjects();
        }
        if (objects == null || objects.isEmpty()) {
            this.objectsAccessed = Collections.emptySet();
        } else {
            this.objectsAccessed = objects;
        }
    }

    /**
     * Restore reconnects to the live metadata objects
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    void restore() throws TeiidComponentException, TeiidProcessingException {
        if (this.objectsAccessed != null) {
            return;
        }
        VDBMetaData vdb = DQPWorkContext.getWorkContext().getVDB();
        TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
        GlobalTableStore globalStore = vdb.getAttachment(GlobalTableStore.class);
        if (!externalNames.isEmpty()) {
            this.objectsAccessed = new HashSet<Object>(externalNames.size());
            for (List<String> key : this.externalNames) {
                if (key.size() == 1) {
                    String matTableName = key.get(0);
                    TempMetadataID id = globalStore.getGlobalTempTableMetadataId(matTableName);
                    if (id == null) {
                        //if the id is null, then create a local instance
                        String viewFullName = matTableName.substring(RelationalPlanner.MAT_PREFIX.length());
                        id = globalStore.getGlobalTempTableMetadataId(tm.getGroupID(viewFullName));
                    }
                    this.objectsAccessed.add(id);
                } else {
                    Schema s = tm.getMetadataStore().getSchema(key.get(0));
                    Modifiable m = s.getTables().get(key.get(1));
                    if (m == null) {
                        m = s.getProcedures().get(key.get(1));
                    }
                    if (m != null) {
                        this.objectsAccessed.add(m);
                    }
                }
            }
        } else {
            this.objectsAccessed = Collections.emptySet();
        }
        this.externalNames = null;
    }

    boolean validate(boolean data, long modTime) {
        if (this.objectsAccessed == null || modTime < 0) {
            return true;
        }
        for (Object o : this.objectsAccessed) {
            if (!data) {
                if (o instanceof Modifiable) {
                    Modifiable m = (Modifiable)o;
                    if (m.getLastModified() < 0) {
                        return false; //invalid object
                    }
                    if (sensitiveToMetadataChanges && m.getLastModified() - modTime >= this.creationTime) {
                        return false;
                    }
                }
            } else if (o instanceof DataModifiable && ((DataModifiable)o).getLastDataModification() - modTime >= this.creationTime) {
                return false;
            }
        }
        return true;
    }

}
