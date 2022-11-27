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

package org.teiid.query.tempdata;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.Assertion;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.util.CommandContext;

/**
 * A wrapper to make TupleSource execution async to the engine.
 * Used only to target what could be cpu intensive temp table queries - which are only expensive to compute up to the first tuple
 */
final class AsyncTupleSource implements TupleSource {
    private TupleSource result;
    private Future<TupleSource> future;
    private Callable<TupleSource> callable;

    public AsyncTupleSource(Callable<TupleSource> callable, CommandContext context) {
        this.callable = callable;
        future = context.submit(callable);
    }

    @Override
    public List<?> nextTuple()
            throws TeiidComponentException, TeiidProcessingException {
        if (future != null) {
            if (!future.isDone()) {
                throw BlockedException.block("waiting for AsyncTupleSource results"); //$NON-NLS-1$
            }
            result = clearFuture();
        }
        Assertion.isNotNull(result);
        synchronized (callable) {
            return result.nextTuple();
        }
    }

    @Override
    public void closeSource() {
        try {
            clearFuture();
        } catch (TeiidComponentException | TeiidProcessingException e) {
            LogManager.logDetail(LogConstants.CTX_DQP, e, "Exeception durring close"); //$NON-NLS-1$
        } finally {
            synchronized (this) {
                result.closeSource();
            }
        }
    }

    private TupleSource clearFuture() throws TeiidComponentException, TeiidProcessingException {
        if (future == null) {
            return null;
        }
        try {
            return future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof BlockedException) {
                throw new AssertionError("Blocking not expected", e); //$NON-NLS-1$
            }
            if (e.getCause() instanceof TeiidComponentException) {
                throw (TeiidComponentException)e.getCause();
            }
            if (e.getCause() instanceof TeiidProcessingException) {
                throw (TeiidProcessingException)e.getCause();
            }
            if (e.getCause() instanceof TeiidRuntimeException) {
                throw (TeiidRuntimeException)e.getCause();
            }
            throw new TeiidRuntimeException(e);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new TeiidRuntimeException(e);
        } finally {
            future = null;
        }
    }
}