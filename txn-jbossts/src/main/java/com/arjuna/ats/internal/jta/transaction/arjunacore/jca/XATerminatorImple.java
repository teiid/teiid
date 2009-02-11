/*
 * JBoss, Home of Professional Open Source
 * Copyright 2006, Red Hat Middleware LLC, and individual contributors 
 * as indicated by the @author tags. 
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors. 
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, 
 * MA  02110-1301, USA.
 * 
 * (C) 2005-2006,
 * @author JBoss Inc.
 */
/*
 * Copyright (C) 2005,
 *
 * Arjuna Technologies Ltd,
 * Newcastle upon Tyne,
 * Tyne and Wear,
 * UK.
 *
 * $Id$
 */

package com.arjuna.ats.internal.jta.transaction.arjunacore.jca;

import java.io.IOException;
import java.util.Stack;

import javax.transaction.HeuristicCommitException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.arjuna.ats.arjuna.common.Uid;
import com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome;
import com.arjuna.ats.arjuna.coordinator.TxControl;
import com.arjuna.ats.arjuna.objectstore.ObjectStore;
import com.arjuna.ats.arjuna.state.InputObjectState;
import com.arjuna.ats.internal.jta.transaction.arjunacore.subordinate.TransactionImple;
import com.arjuna.ats.internal.jta.transaction.arjunacore.subordinate.jca.SubordinateAtomicAction;

/**
 * The XATerminator implementation.
 * 
 * @author mcl
 *
 */

/*
 * See JBTM-456
 */
public class XATerminatorImple implements javax.resource.spi.XATerminator
{

	/**
	 * Commit the transaction identified and hence any inflow-associated work.
	 * 
	 * @param xid
	 *            the transaction to commit
	 * @param onePhase
	 *            whether or not this is a one-phase commit (should only be
	 *            <code>true</code> if there is a single resource associated
	 *            with the transaction).
	 * 
	 * @exception XAException
	 *                thrown if there are any errors, including if the
	 *                transaction cannot commit and has to roll back.
	 */

	public void commit(Xid xid, boolean onePhase) throws XAException
	{
		try
		{
			TransactionImple tx = TxImporter.getImportedTransaction(xid);

			if (tx == null)
				throw new XAException(XAException.XAER_INVAL);

			if (tx.activated())
			{
				if (onePhase)
					tx.doOnePhaseCommit();
				else
					tx.doCommit();

				TxImporter.removeImportedTransaction(xid);
			}
			else
				throw new XAException(XAException.XA_RETRY);
		}
		catch (XAException ex)
		{
			// resource hasn't had a chance to recover yet
			
			if (ex.errorCode != XAException.XA_RETRY)
			{
				TxImporter.removeImportedTransaction(xid);
			}

			throw ex;
		}
		catch (HeuristicRollbackException ex)
		{
			throw new XAException(XAException.XA_HEURRB);
		}
		catch (HeuristicMixedException ex)
		{
			throw new XAException(XAException.XA_HEURMIX);
		}
		catch (SystemException ex)
		{
			TxImporter.removeImportedTransaction(xid);

			throw new XAException(XAException.XAER_RMERR);
		}
	}

	/**
	 * If the transaction subordinate generated a heuristic, then this operation
	 * will be called later once that heuristic has been resolved.
	 * 
	 * @param xid
	 *            the transaction.
	 * 
	 * @throws XAException
	 *             if any error happens.
	 */

	public void forget(Xid xid) throws XAException
	{
		try
		{
			TransactionImple tx = TxImporter.getImportedTransaction(xid);

			if (tx == null)
				throw new XAException(XAException.XAER_INVAL);

			tx.doForget();
		}
		catch (Exception ex)
		{
			throw new XAException(XAException.XAER_RMERR);
		}
		finally
		{
			TxImporter.removeImportedTransaction(xid);
		}
	}

	/**
	 * Prepare the imported transaction.
	 * 
	 * @param xid
	 *            the transaction to prepare.
	 * 
	 * @throws XAException
	 *             thrown if any error occurs, including if the transaction has
	 *             rolled back.
	 * 
	 * @return either XAResource.XA_OK if the transaction prepared, or
	 *         XAResource.XA_RDONLY if it was a read-only transaction (and in
	 *         which case, a second phase message is not expected/required.)
	 */

	public int prepare(Xid xid) throws XAException
	{
		try
		{
			TransactionImple tx = TxImporter.getImportedTransaction(xid);

			if (tx == null)
				throw new XAException(XAException.XAER_INVAL);

			switch (tx.doPrepare())
			{
			case TwoPhaseOutcome.PREPARE_READONLY:
				commit(xid, false);
				return XAResource.XA_RDONLY;
			case TwoPhaseOutcome.PREPARE_NOTOK:
				rollback(xid);
				throw new XAException(XAException.XA_RBROLLBACK);
			case TwoPhaseOutcome.PREPARE_OK:
				return XAResource.XA_OK;
			default:
				throw new XAException(XAException.XA_RBOTHER);
			}
		}
		catch (XAException ex)
		{
			throw ex;
		}
	}

	/**
	 * Return a list of indoubt transactions. This may include those
	 * transactions that are currently in-flight and running 2PC and do not need
	 * recovery invoked on them.
	 * 
	 * @param flag
	 *            either XAResource.TMSTARTRSCAN to indicate the start of a
	 *            recovery scan, or XAResource.TMENDRSCAN to indicate the end of
	 *            the recovery scan.
	 * 
	 * @throws XAException
	 *             thrown if any error occurs.
	 * 
	 * @return a list of potentially indoubt transactions or <code>null</code>.
	 */

	public Xid[] recover(int flag) throws XAException
	{
		/*
		 * Requires going through the objectstore for the states of imported
		 * transactions. Our own crash recovery takes care of transactions
		 * imported via CORBA, Web Services etc.
		 */

		switch (flag)
		{
		case XAResource.TMSTARTRSCAN: // check the object store
			if (_recoveryStarted)
				throw new XAException(XAException.XAER_PROTO);
			else
				_recoveryStarted = true;
			break;
		case XAResource.TMENDRSCAN: // null op for us
			if (_recoveryStarted)
				_recoveryStarted = false;
			else
				throw new XAException(XAException.XAER_PROTO);
			return null;
		case XAResource.TMNOFLAGS:
			if (_recoveryStarted)
				break;
		default:
			throw new XAException(XAException.XAER_PROTO);
		}

		// if we are here, then check the object store

		Xid[] indoubt = null;

		try
		{
			ObjectStore objStore = new ObjectStore(TxControl
					.getActionStoreType());
			InputObjectState states = new InputObjectState();

			// only look in the JCA section of the object store

			if (objStore.allObjUids(SubordinateAtomicAction.getType(), states)
					&& (states.notempty()))
			{
				Stack values = new Stack();
				boolean finished = false;

				do
				{
					Uid uid = new Uid(Uid.nullUid());

					try
					{
						uid.unpack(states);
					}
					catch (IOException ex)
					{
						ex.printStackTrace();

						finished = true;
					}

					if (uid.notEquals(Uid.nullUid()))
					{
						TransactionImple tx = TxImporter
								.recoverTransaction(uid);

						if (tx != null)
							values.push(tx);
					}
					else
						finished = true;

				} while (!finished);

				if (values.size() > 0)
				{
					int index = 0;

					indoubt = new Xid[values.size()];

					while (!values.empty())
					{
						com.arjuna.ats.internal.jta.transaction.arjunacore.subordinate.jca.TransactionImple tx = (com.arjuna.ats.internal.jta.transaction.arjunacore.subordinate.jca.TransactionImple) values
								.pop();

						indoubt[index] = tx.baseXid();
						index++;
					}
				}
			}
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}

		return indoubt;
	}

	/**
	 * Rollback the imported transaction subordinate.
	 * 
	 * @param xid
	 *            the transaction to roll back.
	 * 
	 * @throws XAException
	 *             thrown if there are any errors.
	 */

	public void rollback(Xid xid) throws XAException
	{
		try
		{
			TransactionImple tx = TxImporter.getImportedTransaction(xid);

			if (tx == null)
				throw new XAException(XAException.XAER_INVAL);

			if (tx.activated())
			{
				tx.doRollback();

				TxImporter.removeImportedTransaction(xid);
			}
			else
				throw new XAException(XAException.XA_RETRY);
		}
		catch (XAException ex)
		{
			// resource hasn't had a chance to recover yet
			
			if (ex.errorCode != XAException.XA_RETRY)
			{
				TxImporter.removeImportedTransaction(xid);
			}

			throw ex;
		}
		catch (HeuristicCommitException ex)
		{
			throw new XAException(XAException.XA_HEURCOM);
		}
		catch (HeuristicMixedException ex)
		{
			throw new XAException(XAException.XA_HEURMIX);
		}
		catch (SystemException ex)
		{
			TxImporter.removeImportedTransaction(xid);

			throw new XAException(XAException.XAER_RMERR);
		}
	}

	private boolean _recoveryStarted = false;

}
