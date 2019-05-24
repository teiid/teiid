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

package org.teiid.dqp.service;

import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.common.buffer.impl.BufferManagerImpl;

public class FakeBufferService implements BufferService {

    private BufferManagerImpl bufferMgr;
    private TupleBufferCache tupleBufferCache;

    public FakeBufferService() {
        this(true);
    }

    public FakeBufferService(boolean shared) {
        if (shared) {
            bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
        } else {
            bufferMgr = BufferManagerFactory.createBufferManager();
        }
        this.tupleBufferCache = bufferMgr;
    }

    public FakeBufferService(BufferManagerImpl buffManager, TupleBufferCache tupleBufferCache) {
        this.bufferMgr = buffManager;
        this.tupleBufferCache = tupleBufferCache;
    }

    public BufferManagerImpl getBufferManager() {
        return bufferMgr;
    }

    @Override
    public TupleBufferCache getTupleBufferCache() {
        return tupleBufferCache;
    }

}
