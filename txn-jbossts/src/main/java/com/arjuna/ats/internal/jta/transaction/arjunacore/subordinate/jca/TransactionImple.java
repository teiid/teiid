/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.arjuna.ats.internal.jta.transaction.arjunacore.subordinate.jca;

import com.arjuna.common.util.logging.*;

import com.arjuna.ats.arjuna.common.Uid;
import com.arjuna.ats.jta.logging.*;

import javax.transaction.xa.Xid;

public class TransactionImple
		extends
		com.arjuna.ats.internal.jta.transaction.arjunacore.subordinate.TransactionImple
{

	/**
	 * Create a new transaction with the specified timeout.
	 */

	public TransactionImple(int timeout)
	{
		this(timeout, null);
	}

	public TransactionImple(int timeout, Xid importedXid)
	{
		super(new SubordinateAtomicAction(timeout, importedXid));

		TransactionImple.putTransaction(this);
	}

	/**
	 * Used for failure recovery.
	 * 
	 * @param actId
	 *            the transaction state to recover.
	 */

	public TransactionImple(Uid actId)
	{
		super(new SubordinateAtomicAction(actId));

		// don't put it into list here: it may already be there!
	}

	public final void recordTransaction()
	{
		TransactionImple.putTransaction(this);
	}

	/**
	 * Overloads Object.equals()
	 */

	public boolean equals(Object obj)
	{
		if (jtaLogger.logger.isDebugEnabled())
		{
			jtaLogger.logger.debug(DebugLevel.FUNCTIONS,
					VisibilityLevel.VIS_PUBLIC,
					com.arjuna.ats.jta.logging.FacilityCode.FAC_JTA,
					"TransactionImple.equals");
		}

		if (obj == null)
			return false;

		if (obj == this)
			return true;

		if (obj instanceof TransactionImple)
		{
			return super.equals(obj);
		}

		return false;
	}

	public String toString()
	{
		if (super._theTransaction == null)
			return "TransactionImple < jca-subordinate, NoTransaction >";
		else
		{
			return "TransactionImple < jca-subordinate, "
					+ super._theTransaction + " >";
		}
	}

	/**
	 * If this is an imported transaction (via JCA) then this will be the Xid we
	 * are pretending to be. Otherwise, it will be null.
	 * 
	 * @return null if we are a local transaction, a valid Xid if we have been
	 *         imported.
	 */

	public final Xid baseXid()
	{
		/**
	     * Under imported transaction case, same Oracle instance as multiple 
	     * connectors posed a duplicate XID issue, however with different qualifier it is 
	     * working. 
	     */
		return null;
		//return ((SubordinateAtomicAction) _theTransaction).getXid();
	}

	/**
	 * Force this transaction to try to recover itself again.
	 */

	public void recover()
	{
		_theTransaction.activate();
	}

	/**
	 * Has the transaction been activated successfully? If not, we wait and try
	 * again later.
	 */

	public boolean activated()
	{
		return ((SubordinateAtomicAction) _theTransaction).activated();
	}
}
