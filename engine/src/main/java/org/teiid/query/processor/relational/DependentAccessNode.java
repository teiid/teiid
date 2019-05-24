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

package org.teiid.query.processor.relational;

import java.util.Collections;

import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.Assertion;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;


/**
 * Takes a query with 1 or more dependent sets from 1 or more sources and creates a series of commands. Dependent sets from the
 * same source are treated as a special case. If there are multiple batches from that source, we will create replacement criteria
 * in lock step - rather than forming the full cartesian product space. This implementation assumes that ordering will be
 * respected above the access node and that the incoming dependent tuple values are already sorted.
 */
public class DependentAccessNode extends AccessNode {

    //plan state
    private int maxSetSize;
    private int maxPredicates;
    private boolean pushdown;

    //processing state
    private DependentCriteriaProcessor criteriaProcessor;
    private Criteria dependentCrit;
    private boolean sort = true;
    /**
     * Cached rewritten command to be used as the base for all dependent queries.
     */
    private Command rewrittenCommand;
    private boolean useBindings;
    private boolean complexQuery;

    public DependentAccessNode(int nodeID) {
        super(nodeID);
    }

    /**
     * @see org.teiid.query.processor.relational.AccessNode#close()
     */
    public void closeDirect() {
        super.closeDirect();

        if (criteriaProcessor != null) {
            criteriaProcessor.close();
        }
    }

    public void reset() {
        super.reset();
        criteriaProcessor = null;
        dependentCrit = null;
        sort = true;
        rewrittenCommand = null;
    }

    @Override
    protected Command nextCommand() throws TeiidProcessingException, TeiidComponentException {
        if (rewrittenCommand == null) {
            Command atomicCommand = super.nextCommand();
            try {
                rewriteAndEvaluate(atomicCommand, getEvaluator(Collections.emptyMap()), this.getContext(), this.getContext().getMetadata());
            } catch (BlockedException e) {
                //we must decline already as the parent will assume open, and it will
                //be too late
                if (sort && ((Query)atomicCommand).getOrderBy() != null) {
                    declineSort();
                }
                throw e;
            }
            rewrittenCommand = atomicCommand;
            nextCommand = null;
        }
        if (nextCommand == null && rewrittenCommand != null) {
            nextCommand = (Command)rewrittenCommand.clone();
        }
        return super.nextCommand();
    }

    public Object clone() {
        DependentAccessNode clonedNode = new DependentAccessNode(super.getID());
        clonedNode.maxSetSize = this.maxSetSize;
        clonedNode.maxPredicates = this.maxPredicates;
        clonedNode.pushdown = this.pushdown;
        clonedNode.useBindings = this.useBindings;
        clonedNode.complexQuery = this.complexQuery;
        super.copyTo(clonedNode);
        return clonedNode;
    }

    /**
     * @return Returns the maxSize.
     */
    public int getMaxSetSize() {
        return this.maxSetSize;
    }

    public int getMaxPredicates() {
        return maxPredicates;
    }

    public void setMaxPredicates(int maxPredicates) {
        this.maxPredicates = maxPredicates;
    }

    /**
     * @param maxSize
     *            The maxSize to set.
     */
    public void setMaxSetSize(int maxSize) {
        this.maxSetSize = maxSize;
    }

    /**
     * @see org.teiid.query.processor.relational.AccessNode#prepareNextCommand(org.teiid.query.sql.lang.Command)
     */
    protected boolean prepareNextCommand(Command atomicCommand) throws TeiidComponentException, TeiidProcessingException {

        Assertion.assertTrue(atomicCommand instanceof Query);

        Query query = (Query)atomicCommand;

        try {
            if (this.criteriaProcessor == null) {
                this.criteriaProcessor = new DependentCriteriaProcessor(this.maxSetSize, this.maxPredicates, this, query.getCriteria());
                this.criteriaProcessor.setPushdown(pushdown);
                this.criteriaProcessor.setUseBindings(useBindings);
                this.criteriaProcessor.setComplexQuery(complexQuery);
            }

            if (this.dependentCrit == null) {
                dependentCrit = criteriaProcessor.prepareCriteria();
            }

            query.setCriteria(dependentCrit);
        } catch (BlockedException be) {
            throw new AssertionError("Should not block prior to declining the sort"); //$NON-NLS-1$
            //TODO: the logic could proactively decline the sort rather than throwing an exception
        }

        //walk up the tree and notify the parent join it is responsible for the sort
        if (sort && query.getOrderBy() != null && criteriaProcessor.hasNextCommand()) {
            declineSort();
        }
        if (!sort) {
            query.setOrderBy(null);
        }

        boolean result = RelationalNodeUtil.shouldExecute(atomicCommand, true);

        dependentCrit = null;

        criteriaProcessor.consumedCriteria();

        return result;
    }

    private void declineSort() {
        RelationalNode parent = this.getParent();
        RelationalNode child = this;
        while (parent != null && !(parent instanceof JoinNode)) {
            child = parent;
            if (parent instanceof SortNode) {
                return;
            }
            parent = parent.getParent();
        }
        if (parent != null) {
            JoinNode joinNode = (JoinNode)parent;
            if (joinNode.getJoinStrategy() instanceof MergeJoinStrategy) {
                MergeJoinStrategy mjs = (MergeJoinStrategy)joinNode.getJoinStrategy();
                if (joinNode.getChildren()[0] == child) {
                    mjs.setProcessingSortLeft(true);
                } else {
                    mjs.setProcessingSortRight(true);
                }
            }
        }
        sort = false;
    }

    /**
     * @see org.teiid.query.processor.relational.AccessNode#hasNextCommand()
     */
    protected boolean hasNextCommand() {
        return criteriaProcessor.hasNextCommand();
    }

    public void setPushdown(boolean pushdown) {
        this.pushdown = pushdown;
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        Boolean required = super.requiresTransaction(transactionalReads);
        if (required != null) {
            return required;
        }
        if (transactionalReads || !(this.getCommand() instanceof QueryCommand)) {
            return true;
        }
        return null;
    }

    public boolean isUseBindings() {
        return useBindings;
    }

    public void setUseBindings(boolean useBindings) {
        this.useBindings = useBindings;
    }

    public void setComplexQuery(boolean complexQuery) {
        this.complexQuery = complexQuery;
    }

}