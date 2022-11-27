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

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.query.ObjectReplicator;

class TupleBufferCacheService implements Service<TupleBufferCache>{
    public final InjectedValue<ObjectReplicator> replicatorInjector = new InjectedValue<ObjectReplicator>();
    protected InjectedValue<BufferManager> bufferMgrInjector = new InjectedValue<BufferManager>();

    private TupleBufferCache tupleBufferCache;

    @Override
    public void start(StartContext context) throws StartException {
        if (this.replicatorInjector.getValue() != null) {
            try {
                //use a mux name that will not conflict with any vdb
                this.tupleBufferCache = this.replicatorInjector.getValue().replicate("$TEIID_BM$", TupleBufferCache.class, bufferMgrInjector.getValue(), 0); //$NON-NLS-1$
            } catch (Exception e) {
                throw new StartException(e);
            }
        }
    }

    @Override
    public void stop(StopContext context) {
        if (this.replicatorInjector.getValue() != null && this.tupleBufferCache != null) {
            this.replicatorInjector.getValue().stop(this.tupleBufferCache);
        }
    }

    @Override
    public TupleBufferCache getValue() throws IllegalStateException, IllegalArgumentException {
        if (this.tupleBufferCache!= null) {
            return tupleBufferCache;
        }
        return bufferMgrInjector.getValue();
    }
}
