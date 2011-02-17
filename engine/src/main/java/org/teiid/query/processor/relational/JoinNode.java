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

package org.teiid.query.processor.relational;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.relational.SourceState.ImplicitBuffer;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.util.CommandContext;


/** 
 * @since 4.2
 */
public class JoinNode extends SubqueryAwareRelationalNode {
	
	static class BatchAvailableException extends RuntimeException {}
	
	static BatchAvailableException BATCH_AVILABLE = new BatchAvailableException(); 
	
	public enum JoinStrategyType {    
	    MERGE,
	    PARTITIONED_SORT,
	    NESTED_LOOP,
	    NESTED_TABLE
	}
        
    private enum State { LOAD_LEFT, LOAD_RIGHT, EXECUTE }    
    private State state = State.LOAD_LEFT;
    
    private JoinStrategy joinStrategy;
    private JoinType joinType;
    private String dependentValueSource;
   
    private List leftExpressions;
    private List rightExpressions;
    private boolean leftDistinct;
    private boolean rightDistinct;
    private Criteria joinCriteria;
    
    private Map combinedElementMap;
    private int[] projectionIndexes;
    
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
    
    public boolean isLeftDistinct() {
		return leftDistinct;
	}
    
    public void setLeftDistinct(boolean leftDistinct) {
		this.leftDistinct = leftDistinct;
	}
    
    public boolean isRightDistinct() {
		return rightDistinct;
	}
    
    public void setRightDistinct(boolean rightDistinct) {
		this.rightDistinct = rightDistinct;
	}
    
    public void setJoinCriteria(Criteria joinCriteria) {
        this.joinCriteria = joinCriteria;
    }
    
    @Override
    public void initialize(CommandContext context, BufferManager bufferManager,
    		ProcessorDataManager dataMgr) {
    	super.initialize(context, bufferManager, dataMgr);
    	
    	if (this.combinedElementMap == null) {
	        // Create element lookup map for evaluating project expressions
	        List combinedElements = new ArrayList(getChildren()[0].getElements());
	        combinedElements.addAll(getChildren()[1].getElements());
	        this.combinedElementMap = createLookupMap(combinedElements);
	        this.projectionIndexes = getProjectionIndexes(combinedElementMap, getElements());
    	}
    }
    
    public void open()
        throws TeiidComponentException, TeiidProcessingException {
        // Set Up Join Strategy
        this.joinStrategy.initialize(this);
        
        joinStrategy.openLeft();
        
        if(!isDependent()) {
        	joinStrategy.openRight();
        }
            
        this.state = State.LOAD_LEFT;
    }

    /** 
     * @see org.teiid.query.processor.relational.RelationalNode#clone()
     * @since 4.2
     */
    public Object clone() {
        JoinNode clonedNode = new JoinNode(super.getID());
        super.copy(this, clonedNode);
        
        clonedNode.joinType = this.joinType;
        clonedNode.joinStrategy = this.joinStrategy.clone();
        
        clonedNode.joinCriteria = this.joinCriteria;
        
        clonedNode.leftExpressions = leftExpressions;
        
        clonedNode.rightExpressions = rightExpressions;
        clonedNode.dependentValueSource = this.dependentValueSource;
        clonedNode.rightDistinct = rightDistinct;
        clonedNode.leftDistinct = leftDistinct;
        
        return clonedNode;
    }

    /** 
     * @see org.teiid.query.processor.relational.RelationalNode#nextBatchDirect()
     * @since 4.2
     */
    protected TupleBatch nextBatchDirect() throws BlockedException,
                                          TeiidComponentException,
                                          TeiidProcessingException {
        if (state == State.LOAD_LEFT) {
        	if (this.joinType != JoinType.JOIN_FULL_OUTER) {
            	this.joinStrategy.leftSource.setImplicitBuffer(ImplicitBuffer.NONE);
            }
        	//left child was already opened by the join node
            this.joinStrategy.loadLeft();
            if (isDependent()) { 
                TupleBuffer buffer = this.joinStrategy.leftSource.getTupleBuffer();
                this.getContext().getVariableContext().setGlobalValue(this.dependentValueSource, new DependentValueSource(buffer));
            }
            state = State.LOAD_RIGHT;
        }
        if (state == State.LOAD_RIGHT) {
        	this.joinStrategy.openRight();
            this.joinStrategy.loadRight();
        	this.getContext().getVariableContext().setGlobalValue(this.dependentValueSource, null);
            state = State.EXECUTE;
        }
        try {
        	this.joinStrategy.process();
        	this.terminateBatches();
        } catch (BatchAvailableException e) {
        	//pull the batch
        }
        return pullBatch();
    }

    /** 
     * @see org.teiid.query.processor.relational.RelationalNode#getDescriptionProperties()
     * @since 4.2
     */
    public PlanNode getDescriptionProperties() {
        // Default implementation - should be overridden     
    	PlanNode props = super.getDescriptionProperties();
        
        if(isDependent()) {
        	props.addProperty(PROP_DEPENDENT, Boolean.TRUE.toString());
        }
        props.addProperty(PROP_JOIN_STRATEGY, this.joinStrategy.toString());
        props.addProperty(PROP_JOIN_TYPE, this.joinType.toString());
        List<String> critList = getCriteriaList();
        props.addProperty(PROP_JOIN_CRITERIA, critList);
        return props;
    }

	private List<String> getCriteriaList() {
		List<String> critList = new ArrayList<String>();
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
     * @see org.teiid.query.processor.relational.RelationalNode#getNodeString(java.lang.StringBuffer)
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
    public void setDependentValueSource(String dependentValueSource) {
        this.dependentValueSource = dependentValueSource;
    }
    
    public void closeDirect() {
        super.closeDirect();
        joinStrategy.close();
        if (this.getContext() != null) {
        	this.getContext().getVariableContext().setGlobalValue(this.dependentValueSource, null);
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
    
    boolean matchesCriteria(List outputTuple) throws BlockedException, TeiidComponentException, ExpressionEvaluationException {
		return (this.joinCriteria == null || getEvaluator(this.combinedElementMap).evaluate(this.joinCriteria, outputTuple));
    }

    public List getLeftExpressions() {
        return this.leftExpressions;
    }

    public List getRightExpressions() {
        return this.rightExpressions;
    }
    
    @Override
    protected void addBatchRow(List row) {
        List projectTuple = projectTuple(this.projectionIndexes, row);
        super.addBatchRow(projectTuple);
        if (isBatchFull()) {
        	throw BATCH_AVILABLE;
        }
    }

}
