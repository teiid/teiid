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

package com.metamatrix.query.processor.relational;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.symbol.Expression;

/** 
 * @since 4.2
 */
public class JoinNode extends RelationalNode {
        
    private enum State { LOAD_LEFT, LOAD_RIGHT, EXECUTE }    
    private State state = State.LOAD_LEFT;
    
    private boolean leftOpened;
    private boolean rightOpened;
    
    private JoinStrategy joinStrategy;
    private JoinType joinType;
    private DependentValueSource dependentValueSource;
   
    // Set up state - need to be cloned but not reset
    private List leftExpressions;
    private List rightExpressions;
    private Criteria joinCriteria;
    
    // Cached state - do not need to be cloned or reset
    private Map combinedElementMap;
    
    public JoinNode(int nodeID) {
        super(nodeID);
    }
    
    public void setJoinType(JoinType type) {
        this.joinType = type;
    }
    
    public JoinStrategy getJoinStrategy() {
        return this.joinStrategy;
    }

    public void setJoinStrategy(JoinStrategy joinStrategy) {
        this.joinStrategy = joinStrategy;
    }

    public void setJoinExpressions(List leftExpressions, List rightExpressions) {
        this.leftExpressions = leftExpressions;
        this.rightExpressions = rightExpressions;
    }
    
    public void setJoinCriteria(Criteria joinCriteria) {
        this.joinCriteria = joinCriteria;
    }
    
    /** 
     * @see com.metamatrix.query.processor.relational.RelationalNode#reset()
     */
    @Override
    public void reset() {
        super.reset();
        this.leftOpened = false;
        this.rightOpened = false;
    }

    public void open()
        throws MetaMatrixComponentException, MetaMatrixProcessingException {
        
        // Open left child always
        if (!this.leftOpened) {
            getChildren()[0].open();
            this.leftOpened = true;
        }
        
        if(!isDependent() && !this.rightOpened) {
            // Open right child if non-dependent
            getChildren()[1].open();
            this.rightOpened = true;
        }
            
        // Create element lookup map for evaluating project expressions
        List combinedElements = new ArrayList(getChildren()[0].getElements());
        combinedElements.addAll(getChildren()[1].getElements());
        this.combinedElementMap = createLookupMap(combinedElements);
        this.state = State.LOAD_LEFT;
        // Set Up Join Strategy
        this.joinStrategy.initialize(this);
    }
            
    /** 
     * @see com.metamatrix.query.processor.relational.RelationalNode#clone()
     * @since 4.2
     */
    public Object clone() {
        JoinNode clonedNode = new JoinNode(super.getID());
        super.copy(this, clonedNode);
        
        clonedNode.joinType = this.joinType;
        clonedNode.joinStrategy = (JoinStrategy) this.joinStrategy.clone();
        
        clonedNode.joinCriteria = this.joinCriteria;
        
        if (leftExpressions != null) {
            List leftCopy = new ArrayList(leftExpressions.size());
            for(int i=0; i<leftExpressions.size(); i++) {
                leftCopy.add(((Expression)leftExpressions.get(i)).clone());
            }
            clonedNode.leftExpressions = leftCopy;
        }
        
        if (rightExpressions != null) {
            List rightCopy = new ArrayList(rightExpressions.size());
            for(int i=0; i<rightExpressions.size(); i++) {
                rightCopy.add(((Expression)rightExpressions.get(i)).clone());
            }
            clonedNode.rightExpressions = rightCopy;
        }
        clonedNode.dependentValueSource = this.dependentValueSource;
        
        return clonedNode;
    }

    /** 
     * @see com.metamatrix.query.processor.relational.RelationalNode#nextBatchDirect()
     * @since 4.2
     */
    protected TupleBatch nextBatchDirect() throws BlockedException,
                                          MetaMatrixComponentException,
                                          MetaMatrixProcessingException {
        if (state == State.LOAD_LEFT) {
            //left child was already opened by the join node
            this.joinStrategy.loadLeft();
            state = State.LOAD_RIGHT;
        }
        if (state == State.LOAD_RIGHT) {
            if (isDependent() && !this.rightOpened) { 
                TupleSourceID tsID = this.joinStrategy.leftSource.getTupleSourceID();
                this.dependentValueSource.setTupleSource(this.getBufferManager().getTupleSource(tsID), tsID);
                //open the right side now that the tuples have been collected
                this.getChildren()[1].open();
                this.rightOpened = true;
            }
            this.joinStrategy.loadRight();
            state = State.EXECUTE;
        }
        while(true) {
            if(super.isBatchFull()) {
                return super.pullBatch();
            }
            List outputTuple = this.joinStrategy.nextTuple();
            if(outputTuple != null) {
                List projectTuple = projectTuple(this.combinedElementMap, outputTuple, getElements());
                super.addBatchRow(projectTuple);
            } else {
                super.terminateBatches();
                return super.pullBatch();
            }
        }
    }

    /** 
     * @see com.metamatrix.query.processor.relational.RelationalNode#getDescriptionProperties()
     * @since 4.2
     */
    public Map getDescriptionProperties() {
        // Default implementation - should be overridden     
        Map props = super.getDescriptionProperties();
        
        if(isDependent()) {
            props.put(PROP_TYPE, "Dependent Join"); //$NON-NLS-1$
        } else {
            props.put(PROP_TYPE, "Join"); //$NON-NLS-1$
        }
        props.put(PROP_JOIN_STRATEGY, this.joinStrategy.toString());
        props.put(PROP_JOIN_TYPE, this.joinType.toString());
        List critList = getCriteriaList();
        props.put(PROP_JOIN_CRITERIA, critList);
        return props;
    }

	private List getCriteriaList() {
		List critList = new ArrayList();
        if (leftExpressions != null) {
            for(int i=0; i < this.leftExpressions.size(); i++) {
                critList.add(this.leftExpressions.get(i).toString() + "=" + this.rightExpressions.get(i).toString());  //$NON-NLS-1$
            }
        }
        if (this.joinCriteria != null) {
            for (Criteria crit : (List<Criteria>)Criteria.separateCriteriaByAnd(joinCriteria)) {
                critList.add(crit.toString());
            }
        }
		return critList;
	}

    /** 
     * @see com.metamatrix.query.processor.relational.RelationalNode#getNodeString(java.lang.StringBuffer)
     * @since 4.2
     */
    protected void getNodeString(StringBuffer str) {
        str.append(getClassName());
        str.append("("); //$NON-NLS-1$
        str.append(getID());
        str.append(") [");//$NON-NLS-1$
        if(isDependent()) {
            str.append("Dependent] [");//$NON-NLS-1$
        }
        str.append(this.joinStrategy.toString());
        str.append("] [");//$NON-NLS-1$
        str.append(this.joinType.toString());
        str.append("]"); //$NON-NLS-1$
        if (getJoinType() != JoinType.JOIN_CROSS) {
        	str.append(" criteria=").append(getCriteriaList()); //$NON-NLS-1$
        }
        str.append(" output="); //$NON-NLS-1$
        str.append(getElements());
    }

    /** 
     * @return Returns the isDependent.
     */
    public boolean isDependent() {
        return this.dependentValueSource != null;
    }
    
    /** 
     * @param isDependent The isDependent to set.
     */
    public void setDependentValueSource(DependentValueSource dependentValueSource) {
        this.dependentValueSource = dependentValueSource;
    }
    
    public void close()
            throws MetaMatrixComponentException {
        super.close();
        
        try {
            joinStrategy.close();
        } catch (TupleSourceNotFoundException err) {
            //ignore
        }        
    }

    public JoinType getJoinType() {
        return this.joinType;
    }

    Map getCombinedElementMap() {
        return this.combinedElementMap;
    }

    public Criteria getJoinCriteria() {
        return this.joinCriteria;
    }
    
    boolean matchesCriteria(List outputTuple) throws BlockedException, MetaMatrixComponentException, CriteriaEvaluationException {
        return (this.joinCriteria == null || new Evaluator(this.combinedElementMap, this.getDataManager(), this.getContext()).evaluate(this.joinCriteria, outputTuple));
    }

    public List getLeftExpressions() {
        return this.leftExpressions;
    }

    public List getRightExpressions() {
        return this.rightExpressions;
    }

}
