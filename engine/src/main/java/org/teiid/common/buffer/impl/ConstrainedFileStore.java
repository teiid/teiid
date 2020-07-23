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

package org.teiid.common.buffer.impl;

import java.io.IOException;
import java.lang.ref.WeakReference;

import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.common.buffer.FileStore;
import org.teiid.query.QueryPlugin;

/**
 * Wrapper for BufferManager {@link FileStore} to add max length and other constraints.
 * It removes the implementation for several methods that aren't needed above the
 * buffer manager and removes any synchronization.
 */
class ConstrainedFileStore extends FileStore {

    private static final int SESSION_KILLING_RETRIES = 3;

    private WeakReference<SessionMetadata> session;
    private final FileStore delegate;
    private long maxLength = Long.MAX_VALUE;
    private SessionKiller killer;

    ConstrainedFileStore(FileStore delegate, SessionKiller killer) {
        this.delegate = delegate;
        this.killer = killer;
    }

    @Override
    protected int readWrite(long fileOffset, byte[] b, int offSet, int length,
            boolean write) throws IOException {
        //not needed to be implemented as we have other overrides
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLength(long length) throws IOException {
        //not needed above the buffermanager
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(long start, byte[] bytes, int offset, int length)
            throws IOException {
        //not needed above the buffermanager
        throw new UnsupportedOperationException();
    }

    @Override
    protected void removeDirect() {
        updateBytesUsed(-delegate.getLength());
        delegate.remove();
    }

    @Override
    public int read(long fileOffset, byte[] b, int offSet, int length)
            throws IOException {
        return delegate.read(fileOffset, b, offSet, length);
    }

    @Override
    public void readFully(long fileOffset, byte[] b, int offSet,
            int length) throws IOException {
        delegate.readFully(fileOffset, b, offSet, length);
    }

    @Override
    public void write(byte[] bytes, int offset, int length)
            throws IOException {
        if (maxLength < Long.MAX_VALUE && delegate.getLength() + length > maxLength) {
            throw new IOException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31260, maxLength));
        }
        IOException ex = null;
        for (int i = 0; i < SESSION_KILLING_RETRIES; i++) {
            try {
                delegate.write(bytes, offset, length);
                updateBytesUsed(length);
                return;
            } catch (OutOfDiskException e) {
                ex = e;
                if (!killer.killLargestConsumer()) {
                    break;
                }
            }
        }
        throw ex;
    }

    @Override
    public long getLength() {
        return delegate.getLength();
    }

    public void setMaxLength(long maxLength) {
        this.maxLength = maxLength;
    }

    private final void updateBytesUsed(long length) {
        if (session != null) {
            SessionMetadata s = session.get();
            if (s != null) {
                s.addAndGetBytesUsed(length);
            }
        }
    }

    public void setSession(SessionMetadata session) {
        this.session = new WeakReference<SessionMetadata>(session);
    }

}