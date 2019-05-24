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
package org.teiid.jboss;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.jboss.logging.Logger;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.util.FileUtils;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;


public class ObjectSerializer {

    private static final Logger log = Logger.getLogger(ObjectSerializer.class);

    private static final String ATTACHMENT_SUFFIX = ".ser"; //$NON-NLS-1$

    private String storagePath;

    public ObjectSerializer(String path) {
        this.storagePath = path;
    }

    public <T> T loadAttachment(File attachmentsStore, Class<T> expected) throws IOException, ClassNotFoundException {
        if (log.isTraceEnabled()) {
            log.trace("loadAttachment, attachmentsStore=" + attachmentsStore); //$NON-NLS-1$
        }

        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(new FileInputStream(attachmentsStore));
            return expected.cast(ois.readObject());
        } finally {
            if (ois != null) {
                ois.close();
            }
        }
    }

    public boolean saveAttachment(File attachmentsStore, Object attachment, boolean force) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("saveAttachment, attachmentsStore=" + attachmentsStore + ", attachment=" + attachment); //$NON-NLS-1$ //$NON-NLS-2$
        }

        if (!attachmentsStore.exists() || force) {
            ObjectOutputStream oos = null;
            try {
                attachmentsStore.getParentFile().mkdirs();
                oos = new ObjectOutputStream(new FileOutputStream(attachmentsStore));
                oos.writeObject(attachment);
                return true;
            } finally {
                if (oos != null) {
                    oos.close();
                }
            }
        }
        return false;
    }

    public File buildVDBFile(VDBMetaData vdb) {
        return new File(baseDirectory(vdb.getName()+"_"+vdb.getVersion()), vdb.getName()+"_"+vdb.getVersion()+ATTACHMENT_SUFFIX); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public File buildVdbXml(VDBMetaData vdb) {
        return new File(baseDirectory(vdb.getName()+"_"+vdb.getVersion()), "vdb.xml"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public File buildModelFile(VDBMetaData vdb, String modelName) {
        return new File(baseDirectory(vdb.getName()+"_"+vdb.getVersion()), vdb.getName()+"_"+vdb.getVersion()+"_"+modelName+ATTACHMENT_SUFFIX); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public boolean isStale(VDBMetaData vdb, long timeAfter) {
        File cacheFile = buildVDBFile(vdb);
        return (cacheFile.exists() && timeAfter > cacheFile.lastModified());
    }

    public void removeAttachments(VDBMetaData vdb) {
        String dirName = baseDirectory(vdb.getName()+"_"+vdb.getVersion()); //$NON-NLS-1$
        FileUtils.removeDirectoryAndChildren(new File(dirName));
    }

    public void removeAttachment(File file) {
        FileUtils.remove(file);
    }

    private String baseDirectory(String fileName) {
        String dirName = this.storagePath + File.separator + fileName + File.separator;
        return dirName;
    }

    public <T> T loadSafe(File cacheFile, Class<T> clazz) {
        try {
            if (cacheFile.exists()) {
                return clazz.cast(loadAttachment(cacheFile, clazz));
            }
            return null;
        } catch (Exception e) {
            LogManager.logWarning(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50043, cacheFile.getAbsolutePath()));
        }
        cacheFile.delete();
        return null;
    }

    public OutputStream getVdbXmlOutputStream(VDBMetaData vdb) throws IOException {
        File f = buildVdbXml(vdb);
        if (!f.exists()) {
            f.getParentFile().mkdirs();
        }
        return new FileOutputStream(f);
    }
}
