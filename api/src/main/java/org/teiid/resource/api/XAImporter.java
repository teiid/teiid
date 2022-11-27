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

package org.teiid.resource.api;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

/**
 * Provides the functionality of an XATerminator and the ability to import the relevant
 * transaction.
 */
public interface XAImporter {

    /**
     * Provide the {@link Transaction} for the given {@link Xid}
     * @param transactionManager
     * @param xid
     * @param transactionTimeout
     * @return
     * @throws XAException
     */
    Transaction importTransaction(TransactionManager transactionManager, Xid xid, int transactionTimeout) throws XAException;

    /**
     * Commits the global transaction specified by xid.
     *
     * @param xid A global transaction identifier
     *
     * @param onePhase If true, the resource manager should use a one-phase
     * commit protocol to commit the work done on behalf of xid.
     *
     * @exception XAException An error has occurred. Possible XAExceptions
     * are XA_HEURHAZ, XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR,
     * XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or XAER_PROTO.
     *
     * <P>If the resource manager did not commit the transaction and the
     *  parameter onePhase is set to true, the resource manager may throw
     *  one of the XA_RB* exceptions. Upon return, the resource manager has
     *  rolled back the branch's work and has released all held resources.
     */
    void commit(Xid xid, boolean onePhase) throws XAException;

    /**
     * Tells the resource manager to forget about a heuristically
     * completed transaction branch.
     *
     * @param xid A global transaction identifier.
     *
     * @exception XAException An error has occurred. Possible exception
     * values are XAER_RMERR, XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or
     * XAER_PROTO.
     */
    void forget(Xid xid) throws XAException;

    /**
     * Ask the resource manager to prepare for a transaction commit
     * of the transaction specified in xid.
     *
     * @param xid A global transaction identifier.
     *
     * @exception XAException An error has occurred. Possible exception
     * values are: XA_RB*, XAER_RMERR, XAER_RMFAIL, XAER_NOTA, XAER_INVAL,
     * or XAER_PROTO.
     *
     * @return A value indicating the resource manager's vote on the
     * outcome of the transaction. The possible values are: XA_RDONLY
     * or XA_OK. These constants are defined in
     * <code> javax.transaction.xa.XAResource</code> interface.
     * If the resource manager wants to roll back the
     * transaction, it should do so by raising an appropriate XAException
     * in the prepare method.
     */
    int prepare(Xid xid) throws XAException;

    /**
     * Obtains a list of prepared transaction branches from a resource
     * manager. The transaction manager calls this method during recovery
     * to obtain the list of transaction branches that are currently in
     * prepared or heuristically completed states.
     *
     * @param flag One of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS. TMNOFLAGS
     * must be used when no other flags are set in the parameter. These
     * constants are defined in <code>javax.transaction.xa.XAResource</code>
     * interface.
     *
     * @exception XAException An error has occurred. Possible values are
     * XAER_RMERR, XAER_RMFAIL, XAER_INVAL, and XAER_PROTO.
     *
     * @return The resource manager returns zero or more XIDs of the
     * transaction branches that are currently in a prepared or
     * heuristically completed state. If an error occurs during the
     * operation, the resource manager should throw the appropriate
     * XAException.
     */
    Xid[] recover(int flag) throws XAException;

    /**
     * Informs the resource manager to roll back work done on behalf
     * of a transaction branch.
     *
     * @param xid A global transaction identifier.
     *
     * @exception XAException An error has occurred. Possible XAExceptions are
     * XA_HEURHAZ, XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR, XAER_RMFAIL,
     * XAER_NOTA, XAER_INVAL, or XAER_PROTO.
     *
     * <p>If the transaction branch is already marked rollback-only the
     * resource manager may throw one of the XA_RB* exceptions. Upon return,
     * the resource manager has rolled back the branch's work and has released
     * all held resources.
     */
    void rollback(Xid xid) throws XAException;

}
