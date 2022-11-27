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

package org.teiid.common.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.serial.SerialBlob;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.BaseClobType;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.InputStreamFactory.BlobInputStreamFactory;
import org.teiid.core.types.InputStreamFactory.ClobInputStreamFactory;
import org.teiid.core.types.InputStreamFactory.SQLXMLInputStreamFactory;
import org.teiid.core.types.InputStreamFactory.StorageMode;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.Expression;

/**
 * Tracks lob references so they are not lost during serialization.
 * TODO: for temp tables we may need to have a copy by value management strategy
 */
public class LobManager {

    public enum ReferenceMode {
        ATTACH,
        CREATE,
        REMOVE,
    }

    private static class LobHolder {
        Streamable<?> lob;
        int referenceCount = 1;

        public LobHolder(Streamable<?> lob) {
            this.lob = lob;
        }
    }

    private Map<String, LobHolder> lobReferences = Collections.synchronizedMap(new HashMap<String, LobHolder>());
    private boolean inlineLobs = true;
    private int maxMemoryBytes = DataTypeManager.MAX_LOB_MEMORY_BYTES;
    private int[] lobIndexes;
    private FileStore lobStore;
    private boolean saveTemporary;

    public LobManager(int[] lobIndexes, FileStore lobStore) {
        this.lobIndexes = lobIndexes;
        this.lobStore = lobStore;
    }

    public LobManager clone() {
        LobManager clone = new LobManager(lobIndexes, null);
        clone.inlineLobs = inlineLobs;
        clone.maxMemoryBytes = maxMemoryBytes;
        clone.saveTemporary = saveTemporary;
        synchronized (lobReferences) {
            for (Map.Entry<String, LobHolder> entry : lobReferences.entrySet()) {
                LobHolder lobHolder = new LobHolder(entry.getValue().lob);
                lobHolder.referenceCount = entry.getValue().referenceCount;
                clone.lobReferences.put(entry.getKey(), lobHolder);
            }
        }
        return clone;
    }

    public void setInlineLobs(boolean trackMemoryLobs) {
        this.inlineLobs = trackMemoryLobs;
    }

    public void setMaxMemoryBytes(int maxMemoryBytes) {
        this.maxMemoryBytes = maxMemoryBytes;
    }

    @SuppressWarnings("unchecked")
    public void updateReferences(List<?> tuple, ReferenceMode mode)
            throws TeiidComponentException {
        for (int i = 0; i < lobIndexes.length; i++) {
            Object anObj = tuple.get(lobIndexes[i]);
            if (!(anObj instanceof Streamable<?>)) {
                continue;
            }
            Streamable lob = (Streamable) anObj;
            String id = lob.getReferenceStreamId();
            LobHolder lobHolder = this.lobReferences.get(id);
            switch (mode) {
            case REMOVE:
                if (lobHolder != null) {
                    lobHolder.referenceCount--;
                    if (lobHolder.referenceCount < 1) {
                        this.lobReferences.remove(id);
                    }
                }
                break;
            case ATTACH:
                if (lob.getReference() == null) {
                    if (lobHolder == null) {
                         throw new TeiidComponentException(QueryPlugin.Event.TEIID30033, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30033));
                    }
                    lob.setReference(lobHolder.lob.getReference());
                }
                break;
            case CREATE:
                try {
                    StorageMode storageMode = InputStreamFactory.getStorageMode(lob);
                    if (id == null || (inlineLobs
                            && (storageMode == StorageMode.MEMORY
                            || (storageMode != StorageMode.FREE && lob.length()*(lob instanceof BaseClobType?2:1) <= maxMemoryBytes)))) {
                        lob.setReferenceStreamId(null);
                        //since this is untracked at this point, we must detach if possible
                        if (inlineLobs && storageMode == StorageMode.OTHER) {
                            persistLob(lob, null, null, true, maxMemoryBytes);
                        } else {
                            saveTemporaryLob(lob);
                        }
                        continue;
                    }
                } catch (SQLException e) {
                    //presumably the lob is bad, but let it slide for now
                }
                if (lob.getReference() == null) {
                     throw new TeiidComponentException(QueryPlugin.Event.TEIID30034, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30034));
                }
                if (lobHolder == null) {
                    saveTemporaryLob(lob);
                    this.lobReferences.put(id, new LobHolder(lob));
                } else {
                    lobHolder.referenceCount++;
                }
                break;
            }
        }
    }

    private void saveTemporaryLob(Streamable<?> lob) {
        if (saveTemporary) {
            InputStreamFactory.setTemporary(lob, false);
        }
    }

    public Streamable<?> getLobReference(String id) throws TeiidComponentException {
        LobHolder lob = this.lobReferences.get(id);
        if (lob == null) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30035, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30035));
        }
        return lob.lob;
    }

    public static int[] getLobIndexes(List<? extends Expression> expressions) {
        if (expressions == null) {
            return null;
        }
        int[] result = new int[expressions.size()];
        int resultIndex = 0;
        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = expressions.get(i);
            if (DataTypeManager.isLOB(expr.getType()) || expr.getType() == DataTypeManager.DefaultDataClasses.OBJECT) {
                result[resultIndex++] = i;
            }
        }
        if (resultIndex == 0) {
            return null;
        }
        return Arrays.copyOf(result, resultIndex);
    }

    public void persist() throws TeiidComponentException {
        // stream the contents of lob into file store.
        byte[] bytes = new byte[1 << 14];
        AutoCleanupUtil.setCleanupReference(this, lobStore);
        for (Map.Entry<String, LobHolder> entry : this.lobReferences.entrySet()) {
            detachLob(entry.getValue().lob, lobStore, bytes);
        }
    }

    public void detachLob(final Streamable<?> lob, final FileStore store, byte[] bytes) throws TeiidComponentException {
        // if this is not attached, just return
        if (InputStreamFactory.getStorageMode(lob) != StorageMode.MEMORY) {
            persistLob(lob, store, bytes, inlineLobs, maxMemoryBytes);
        } else {
            InputStreamFactory.setTemporary(lob, false);
        }
    }

    public static void persistLob(final Streamable<?> lob,
            final FileStore store, byte[] bytes, boolean inlineLobs, int maxMemoryBytes) throws TeiidComponentException {
        long byteLength = Integer.MAX_VALUE;

        try {
            byteLength = lob.length()*(lob instanceof BaseClobType?2:1);
        } catch (SQLException e) {
            //just ignore for now - for a single read resource computing the length invalidates
            //TODO - inline small persisted lobs
        }

        try {
            //inline
            if (lob.getReferenceStreamId() == null || (inlineLobs
                    && (byteLength <= maxMemoryBytes))) {
                lob.setReferenceStreamId(null);
                if (InputStreamFactory.getStorageMode(lob) == StorageMode.MEMORY) {
                    return;
                }

                if (lob instanceof BlobType) {
                    BlobType b = (BlobType)lob;
                    byte[] blobBytes = b.getBytes(1, (int)byteLength);
                    b.setReference(new SerialBlob(blobBytes));
                } else if (lob instanceof BaseClobType) {
                    BaseClobType c = (BaseClobType)lob;
                    String s = ""; //$NON-NLS-1$
                    //some clob impls return null for 0 length
                    if (byteLength != 0) {
                        s = c.getSubString(1, (int)(byteLength>>>1));
                    }
                    c.setReference(new ClobImpl(s));
                } else {
                    XMLType x = (XMLType)lob;
                    String s = x.getString();
                    x.setReference(new SQLXMLImpl(s));
                }

                return;
            }

            InputStream is = null;
            if (lob instanceof BlobType) {
                is = new BlobInputStreamFactory((Blob)lob).getInputStream();
            } else if (lob instanceof BaseClobType) {
                is = new ClobInputStreamFactory((Clob)lob).getInputStream();
            } else {
                is = new SQLXMLInputStreamFactory((SQLXML)lob).getInputStream();
            }

            long offset = store.getLength();

            OutputStream fsos = store.createOutputStream();
            byteLength = ObjectConverterUtil.write(fsos, is, bytes, -1);

            // re-construct the new lobs based on the file store
            final long lobOffset = offset;
            final long lobLength = byteLength;
            /*
             * Using an inner class here will hold a reference to the LobManager
             * which prevents the removal of the FileStore until all of the
             * lobs have been gc'd
             */
            InputStreamFactory isf = new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return store.createInputStream(lobOffset, lobLength);
                }

                @Override
                public StorageMode getStorageMode() {
                    return StorageMode.PERSISTENT;
                }
            };
            isf.setLength(byteLength);
            if (lob instanceof BlobType) {
                ((BlobType)lob).setReference(new BlobImpl(isf));
            }
            else if (lob instanceof BaseClobType) {
                long length = -1;
                try {
                    length = ((BaseClobType)lob).length();
                } catch (SQLException e) {
                    //could be streaming
                }
                ((BaseClobType)lob).setReference(new ClobImpl(isf, length));
            }
            else {
                ((XMLType)lob).setReference(new SQLXMLImpl(isf));
            }
        } catch (SQLException e) {
            throw new TeiidComponentException(QueryPlugin.Event.TEIID30037, e);
        } catch (IOException e) {
            throw new TeiidComponentException(QueryPlugin.Event.TEIID30036, e);
        }
    }

    public int getLobCount() {
        return this.lobReferences.size();
    }

    public void remove() {
        this.lobReferences.clear();
        if (this.lobStore != null) {
            this.lobStore.remove();
        }
    }

    public void setSaveTemporary(boolean b) {
        this.saveTemporary = b;
    }
}
