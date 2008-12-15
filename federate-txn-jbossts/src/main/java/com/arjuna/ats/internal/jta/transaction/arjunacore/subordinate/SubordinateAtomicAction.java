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
 * Copyright (C) 2003,
 * 
 * Hewlett-Packard Arjuna Labs, Newcastle upon Tyne, Tyne and Wear, UK.
 * 
 * $Id$
 */

package com.arjuna.ats.internal.jta.transaction.arjunacore.subordinate;

import com.arjuna.ats.arjuna.AtomicAction;
import com.arjuna.ats.arjuna.common.Uid;
import com.arjuna.ats.arjuna.coordinator.ActionStatus;
import com.arjuna.ats.arjuna.coordinator.TransactionReaper;
import com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome;

/**
 * A subordinate JTA transaction; used when importing another transaction
 * context.
 * 
 * @author mcl
 */

/*
 * see JBTM-457
 */
public class SubordinateAtomicAction extends
        com.arjuna.ats.internal.jta.transaction.arjunacore.AtomicAction
{

    public SubordinateAtomicAction ()
    {
        super();
        
        start();
    }

    public SubordinateAtomicAction (int timeout)
    {
        super();
        
        start();
        
        // if it has a non-negative timeout, add it to the reaper.
        
        if (timeout > AtomicAction.NO_TIMEOUT)
            TransactionReaper.transactionReaper(true).insert(this, timeout);
    }

    /**
     * Commit the transaction, and have heuristic reporting. Heuristic reporting
     * via the return code is enabled.
     * 
     * @return <code>ActionStatus</code> indicating outcome.
     */

    public int commit ()
    {
        return ActionStatus.INVALID;
    }

    /**
     * Commit the transaction. The report_heuristics parameter can be used to
     * determine whether or not heuristic outcomes are reported.
     * 
     * If the transaction has already terminated, or has not begun, then an
     * appropriate error code will be returned.
     * 
     * @return <code>ActionStatus</code> indicating outcome.
     */

    public int commit (boolean report_heuristics)
    {
        return ActionStatus.INVALID;
    }

    /**
     * Abort (rollback) the transaction.
     * 
     * If the transaction has already terminated, or has not been begun, then an
     * appropriate error code will be returned.
     * 
     * @return <code>ActionStatus</code> indicating outcome.
     */

    public int abort ()
    {
        return ActionStatus.INVALID;
    }

    /**
     * The type of the class is used to locate the state of the transaction log
     * in the object store.
     * 
     * Overloads BasicAction.type()
     * 
     * @return a string representation of the hierarchy of the class for storing
     *         logs in the transaction object store.
     */

    // TODO crash recovery!!!!
    
    public String type ()
    {
        return "/StateManager/BasicAction/TwoPhaseCoordinator/AtomicAction/SubordinateAtomicAction"; //$NON-NLS-1$
    }

    public int doPrepare ()
    {
        if (super.beforeCompletion()) {
            return super.prepare(true);
        }
        super.phase2Abort(true);
        
        return TwoPhaseOutcome.PREPARE_NOTOK;
    }

    public int doCommit ()
    {
        super.phase2Commit(true);

        int toReturn;
        
        switch (super.getHeuristicDecision())
        {
        case TwoPhaseOutcome.PREPARE_OK:
        case TwoPhaseOutcome.FINISH_OK:
            toReturn = super.status();      
            break;
        case TwoPhaseOutcome.HEURISTIC_ROLLBACK:
            toReturn = ActionStatus.H_ROLLBACK;
            break;
        case TwoPhaseOutcome.HEURISTIC_COMMIT:
            toReturn = ActionStatus.H_COMMIT;
            break;
        case TwoPhaseOutcome.HEURISTIC_MIXED:
            toReturn = ActionStatus.H_MIXED;
            break;
        case TwoPhaseOutcome.HEURISTIC_HAZARD:
        default:
            toReturn = ActionStatus.H_HAZARD;
            break;
        }
        
        super.afterCompletion(toReturn);
        
        return toReturn;
    }

    public int doRollback ()
    {
        super.phase2Abort(true);
        
        int toReturn;
        
        switch (super.getHeuristicDecision())
        {
        case TwoPhaseOutcome.PREPARE_OK:
        case TwoPhaseOutcome.FINISH_OK:
            toReturn = super.status();
            break;
        case TwoPhaseOutcome.HEURISTIC_ROLLBACK:
            toReturn = ActionStatus.H_ROLLBACK;
            break;
        case TwoPhaseOutcome.HEURISTIC_COMMIT:
            toReturn = ActionStatus.H_COMMIT;
            break;
        case TwoPhaseOutcome.HEURISTIC_MIXED:
            toReturn = ActionStatus.H_MIXED;
            break;
        case TwoPhaseOutcome.HEURISTIC_HAZARD:
        default:
            toReturn = ActionStatus.H_HAZARD;
            break;
        }
        
        super.afterCompletion(toReturn);
        
        return toReturn;
    }

    public int doOnePhaseCommit ()
    {
        int result = super.End(true);
        
        super.afterCompletion(result);
        
        return result;
    }

    public void doForget ()
    {
        super.forgetHeuristics();
        
        doRollback();
    }
    
    /**
     * For crash recovery purposes.
     * 
     * @param actId the identifier to recover.
     */
    
    protected SubordinateAtomicAction (Uid actId)
    {
        super(actId);
    }
    
    /**
     * By default the BasicAction class only allows the termination of a
     * transaction if it's the one currently associated with the thread. We
     * override this here.
     * 
     * @return <code>false</code> to indicate that this transaction can only
     *         be terminated by the right thread.
     */

    protected boolean checkForCurrent ()
    {
        return false;
    }
    
}
