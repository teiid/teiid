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

import java.util.Collection;
import java.util.List;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.translator.CacheDirective;
import org.teiid.translator.CacheDirective.Scope;

/**
 * A proxy {@link TupleSource} that caches a {@link DataTierTupleSource}
 */
final class CachingTupleSource extends
        TupleSourceCache.CopyOnReadTupleSource {
    private final DataTierManagerImpl dataTierManagerImpl;
    private final CacheID cid;
    private final RegisterRequestParameter parameterObject;
    private final CacheDirective cd;
    private final Collection<GroupSymbol> accessedGroups;
    final DataTierTupleSource dtts;
    final RequestWorkItem item;

    CachingTupleSource(DataTierManagerImpl dataTierManagerImpl, TupleBuffer tb, DataTierTupleSource ts, CacheID cid,
            RegisterRequestParameter parameterObject, CacheDirective cd,
            Collection<GroupSymbol> accessedGroups, RequestWorkItem item) {
        super(tb, ts);
        this.dataTierManagerImpl = dataTierManagerImpl;
        this.dtts = ts;
        this.cid = cid;
        this.parameterObject = parameterObject;
        this.cd = cd;
        this.accessedGroups = accessedGroups;
        this.item = item;
    }

    @Override
    public List<?> nextTuple() throws TeiidComponentException,
            TeiidProcessingException {
        if (dtts.scope == Scope.NONE || tb == null) {
            removeTupleBuffer();
            return ts.nextTuple();
        }
        //TODO: the cache directive object needs synchronized for consistency
        List<?> tuple = super.nextTuple();
        if (tuple == null && !dtts.errored) {
            synchronized (cd) {
                if (dtts.scope == Scope.NONE) {
                    removeTupleBuffer();
                    return tuple;
                }
                CachedResults cr = new CachedResults();
                cr.setResults(tb, null);
                if (!Boolean.FALSE.equals(cd.getUpdatable())) {
                    if (accessedGroups != null) {
                        for (GroupSymbol gs : accessedGroups) {
                            cr.getAccessInfo().addAccessedObject(gs.getMetadataID());
                        }
                    }
                } else {
                    cr.getAccessInfo().setSensitiveToMetadataChanges(false);
                }
                if (parameterObject.limit > 0 && parameterObject.limit == rowNumber) {
                    cr.setRowLimit(rowNumber);
                }
                tb.setPrefersMemory(Boolean.TRUE.equals(cd.getPrefersMemory()));
                Determinism determinismLevel = getDeterminismLevel(this.dtts.scope);
                this.dataTierManagerImpl.requestMgr.getRsCache().put(cid, determinismLevel, cr, cd.getTtl());
                tb = null;
            }
        }
        return tuple;
    }

    public static Determinism getDeterminismLevel(CacheDirective.Scope scope) {
        Determinism determinismLevel = Determinism.SESSION_DETERMINISTIC;
        if (scope != null) {
            switch (scope) {
            case VDB:
                determinismLevel = Determinism.VDB_DETERMINISTIC;
                break;
            case SESSION:
                determinismLevel = Determinism.SESSION_DETERMINISTIC;
                break;
            case USER:
                determinismLevel = Determinism.USER_DETERMINISTIC;
                break;
            }
        }
        return determinismLevel;
    }

    private void removeTupleBuffer() {
        if (tb != null) {
            tb.remove();
            tb = null;
        }
    }

    @Override
    public void closeSource() {
        try {
            if (tb != null && !dtts.errored) {
                boolean readAll = true;
                synchronized (cd) {
                    readAll = !Boolean.FALSE.equals(cd.getReadAll());
                }
                if (readAll) {
                    //TODO that this is blocking, so it could be made faster in non-transactional scenarios
                    //we should also shut off any warnings, since the plan isn't consuming these tuples
                    //the approach would probably be to do more read-ahead
                    dtts.getAtomicRequestMessage().setSerial(true);
                    while (dtts.scope != Scope.NONE) {
                        if (item.isCanceled()) {
                            LogManager.logDetail(LogConstants.CTX_DQP, dtts.getAtomicRequestMessage().getAtomicRequestID(), "Not using full results due to cancellation."); //$NON-NLS-1$
                            break;
                        }
                        try {
                            List<?> tuple = nextTuple();
                            if (tuple == null) {
                                break;
                            }
                        } catch (BlockedException e) {
                            //this is possible if were were already waiting for an asynch result
                            try {
                                Thread.sleep(50); //TODO: we could synch/notify in the DataTierTupleSource
                            } catch (InterruptedException e1) {
                                break;
                            }
                        } catch (TeiidException e) {
                            LogManager.logDetail(LogConstants.CTX_DQP, e, dtts.getAtomicRequestMessage().getAtomicRequestID(), "Not using full results due to error."); //$NON-NLS-1$
                            break;
                        }
                    }
                }
            }
        } finally {
            removeTupleBuffer();
            ts.closeSource();
        }
    }
}