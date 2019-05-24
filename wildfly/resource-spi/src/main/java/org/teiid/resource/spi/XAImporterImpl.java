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

package org.teiid.resource.spi;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import javax.resource.NotSupportedException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.work.ExecutionContext;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkManager;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.teiid.resource.api.XAImporter;

/**
 * Simple {@link XAImporter} implementations based upon an {@link XATerminator} and {@link WorkManager}
 */
public class XAImporterImpl implements XAImporter {

    private static class FutureWork<T> extends FutureTask<T> implements Work {
        public FutureWork(Callable<T> callable) {
            super(callable);
        }

        @Override
        public void release() {

        }
    }

    private XATerminator xaTerminator;
    private WorkManager workManager;

    public XAImporterImpl(XATerminator xaTerminator, WorkManager workManager) {
        this.xaTerminator = xaTerminator;
        this.workManager = workManager;
    }

    @Override
    public Transaction importTransaction(TransactionManager transactionManager,
            Xid xid, int transactionTimeout) throws XAException {
        ExecutionContext ec = new ExecutionContext();
        ec.setXid(xid);
        try {
            ec.setTransactionTimeout(transactionTimeout);

            FutureWork<Transaction> work = new FutureWork<>(new Callable<Transaction>() {
                @Override
                public Transaction call() throws Exception {
                    return transactionManager.getTransaction();
                }
            });
            workManager.doWork(work, WorkManager.INDEFINITE, ec, null);
            return work.get();
        } catch (ExecutionException | NotSupportedException | WorkException | InterruptedException e) {
            XAException xaException = new XAException();
            xaException.initCause(e);
            throw xaException;
        }
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        xaTerminator.commit(xid, onePhase);
    }

    @Override
    public void forget(Xid xid) throws XAException {
        xaTerminator.forget(xid);
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        return xaTerminator.prepare(xid);
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        return xaTerminator.recover(flag);
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        xaTerminator.rollback(xid);
    }

}
