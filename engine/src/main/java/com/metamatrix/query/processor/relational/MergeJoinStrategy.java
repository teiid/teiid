/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.Collections;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.OrderBy;

/**
 * MergeJoinStrategy supports generalized Full, Left Outer, and Inner Joins (containing non-equi join criteria) as long as there
 * is at least one equi-join criteria
 * 
 * Additionally supports Semi and Anti-Semi Joins.  These too allow for generalized non-equi join criteria.
 * TODO: when there is no non-equi join criteria matching duplicates from the outer side can be output immediately 
 * TODO: semi joins should only output left tuples
 * 
 * Support for Intersect and Except is controlled by the grouping flag, which changes comparisons to check
 * for null equality. 
 * 
 */
public class MergeJoinStrategy extends JoinStrategy {

    private enum MergeState {
        SCAN, MATCH, DONE
    }
    
    private enum ScanState { //left and right states within the MergeState.SCAN
        READ, KEEP, DONE
    }
    
    private enum MatchState { // states within the MergeState.MATCH
        MATCH_LEFT, MATCH_RIGHT //match right is only used on full outer joins
    }

    private enum LoopState { //inner loop states within the MergeState.MATCH
        LOAD_OUTER, LOAD_INNER, EVALUATE_CRITERIA
    }
    
    private MergeState mergeState = MergeState.SCAN;
    private ScanState leftScanState = ScanState.READ;
    private ScanState rightScanState = ScanState.READ;
    private MatchState matchState = MatchState.MATCH_LEFT;
    private LoopState loopState = LoopState.EVALUATE_CRITERIA;
  
    //match state members
    private SourceState outerState;
    private SourceState innerState;
    private boolean outerMatched;
    
    //planning time information
    public enum SortOption {
        SKIP_SORT, SORT, SORT_DISTINCT
    }
    
    private SortOption sortLeft;
    private SortOption sortRight;
        
    /** false if three-level comparison, true if grouping comparison (null == null) */
    private boolean grouping;

    //load time state
    private SortUtility leftSort;
    private SortUtility rightSort;
    private SortOption processingSortRight;   
    
    public MergeJoinStrategy(boolean sortLeft, boolean sortRight) {
        this(sortLeft?SortOption.SORT:SortOption.SKIP_SORT, sortRight?SortOption.SORT:SortOption.SKIP_SORT, false);
    }
    
    public MergeJoinStrategy(SortOption sortLeft, SortOption sortRight, boolean grouping) {
        this.sortLeft = sortLeft;
        this.sortRight = sortRight;
        this.grouping = grouping;
    }

    /**
     * @see com.metamatrix.query.processor.relational.JoinStrategy#clone()
     */
    @Override
    public Object clone() {
        return new MergeJoinStrategy(sortLeft, sortRight, grouping);
    }

    /**
     * @see com.metamatrix.query.processor.relational.JoinStrategy#initialize(com.metamatrix.query.processor.relational.JoinNode)
     */
    @Override
    public void initialize(JoinNode joinNode) throws MetaMatrixComponentException {
        super.initialize(joinNode);
        this.outerState = this.leftSource;
        this.innerState = this.rightSource;
        this.mergeState = MergeState.SCAN;
        this.matchState = MatchState.MATCH_LEFT;
        this.loopState = LoopState.EVALUATE_CRITERIA;
        this.leftScanState = ScanState.READ;
        this.rightScanState = ScanState.READ;
        this.outerMatched = false;
        this.processingSortRight = this.sortRight;
    }

    /**
     * @see com.metamatrix.query.processor.relational.JoinStrategy#close()
     */
    @Override
    public void close() throws TupleSourceNotFoundException,
                       MetaMatrixComponentException {
        super.close();
        this.outerState = null;
        this.innerState = null;
        this.leftSort = null;
        this.rightSort = null;
    }

    protected List nextTuple() throws MetaMatrixComponentException,
                              CriteriaEvaluationException,
                              MetaMatrixProcessingException {
        while (this.mergeState != MergeState.DONE) {
            
            while (this.mergeState == MergeState.SCAN) {
                if (this.leftScanState == ScanState.READ) {
                    if (this.leftSource.getIterator().hasNext()) {
                        this.leftSource.getIterator().mark();
                        this.leftSource.saveNext();
                        this.leftScanState = ScanState.KEEP;
                    } else {
                        this.leftScanState = ScanState.DONE;
                        if (joinNode.getJoinType() != JoinType.JOIN_FULL_OUTER) {
                            mergeState = MergeState.DONE;
                            return null;
                        }
                    }
                }

                if (rightScanState == ScanState.READ) {
                    if (this.rightSource.getIterator().hasNext()) {
                        this.rightSource.getIterator().mark();
                        this.rightSource.saveNext();
                        rightScanState = ScanState.KEEP;
                    } else {
                        this.rightScanState = ScanState.DONE;
                        if (!this.joinNode.getJoinType().isOuter()) {
                            mergeState = MergeState.DONE;
                            return null;
                        }
                    }
                }
                
                int result = 0;
                
                if (this.leftScanState == ScanState.DONE) {
                    if (this.rightScanState == ScanState.DONE) {
                        this.mergeState = MergeState.DONE;
                        return null;
                    }
                    result = -1;
                } else if (this.rightScanState == ScanState.DONE) {
                    result = 1;
                } else {
                    result = compare(this.leftSource.getCurrentTuple(), this.rightSource.getCurrentTuple(), this.leftSource.getExpressionIndexes(), this.rightSource.getExpressionIndexes());
                }

                if (result == 0) {
                    this.mergeState = MergeState.MATCH; //switch to nested loop algorithm
                    this.loopState = LoopState.EVALUATE_CRITERIA;
                    this.leftScanState = ScanState.READ;
                    this.rightScanState = ScanState.READ;
                    break;
                } else if (result > 0) {
                    this.leftScanState = ScanState.READ;
                    if (this.joinNode.getJoinType().isOuter()) {
                        return outputTuple(this.leftSource.getCurrentTuple(), this.rightSource.getOuterVals());
                    }
                } else {
                    this.rightScanState = ScanState.READ;
                    if (joinNode.getJoinType() == JoinType.JOIN_FULL_OUTER) {
                        return outputTuple(this.leftSource.getOuterVals(), this.rightSource.getCurrentTuple());
                    }
                }
            }

            while (this.mergeState == MergeState.MATCH) {
                if (loopState == LoopState.LOAD_OUTER) {
                    if (compareToPrevious(outerState)) {
                        outerMatched = false;
                        innerState.reset();
                        loopState = LoopState.LOAD_INNER;
                    } else if (matchState == MatchState.MATCH_LEFT && joinNode.getJoinType() == JoinType.JOIN_FULL_OUTER) {
                        // on a full outer join, we need to determine the outer right values as well
                        matchState = MatchState.MATCH_RIGHT;
                        outerState = this.rightSource;
                        innerState = this.leftSource;
                        outerState.reset();
                        continue;
                    }  else { 
                        //exit match region and restore values
                        outerState = this.leftSource;
                        innerState = this.rightSource;
                        outerMatched = false;
                        this.leftSource.getIterator().setPosition(this.leftSource.getMaxProbeMatch());
                        this.rightSource.getIterator().setPosition(this.rightSource.getMaxProbeMatch());
                        this.mergeState = MergeState.SCAN;
                        this.matchState = MatchState.MATCH_LEFT;
                        break;
                    }
                }

                while (loopState == LoopState.LOAD_INNER || loopState == LoopState.EVALUATE_CRITERIA) {
                    if (loopState == LoopState.LOAD_INNER) {
                        if (compareToPrevious(innerState)) {
                            loopState = LoopState.EVALUATE_CRITERIA;
                        } else {
                            loopState = LoopState.LOAD_OUTER;
                        }
                    }

                    if (loopState == LoopState.EVALUATE_CRITERIA) {
                        List outputTuple = outputTuple(this.leftSource.getCurrentTuple(), this.rightSource.getCurrentTuple());
                        
                        boolean match = this.joinNode.matchesCriteria(outputTuple);
                        loopState = LoopState.LOAD_INNER; // now that the criteria has been evaluated, try the next inner value

                        if (match) {
                            outerMatched = true;
                            if (matchState == MatchState.MATCH_LEFT && this.joinNode.getJoinType() != JoinType.JOIN_ANTI_SEMI) {
                                if (this.joinNode.getJoinType() == JoinType.JOIN_SEMI) {
                                    this.loopState = LoopState.LOAD_OUTER; //only one match is needed for semi join
                                } 
                                return outputTuple;
                            }
                            //right outer join || anti semi join can skip to the next outer value
                            this.loopState = LoopState.LOAD_OUTER;
                        } 
                    }
                }

                if (!outerMatched) {
                    if (matchState == MatchState.MATCH_LEFT) {
                        if (this.joinNode.getJoinType().isOuter()) {
                            return outputTuple(this.leftSource.getCurrentTuple(), this.rightSource.getOuterVals());
                        }
                    } else if (joinNode.getJoinType() == JoinType.JOIN_FULL_OUTER) {
                        return outputTuple(this.leftSource.getOuterVals(), this.rightSource.getCurrentTuple());
                    }
                }
            }
        }
        return null;
    }

    protected boolean compareToPrevious(SourceState target) throws MetaMatrixComponentException,
                                    MetaMatrixProcessingException {
        if (!target.getIterator().hasNext()) {
            target.setMaxProbeMatch(target.getIterator().getCurrentIndex());
            return false;
        }
        List previousTuple = target.getCurrentTuple();
        target.saveNext();
        if (target.getMaxProbeMatch() >= target.getIterator().getCurrentIndex()) {
            return true;
        }
        if (previousTuple != null) {
            int compare = 1;
            if (!target.isDistinct()) {
                compare = compare(previousTuple, target.getCurrentTuple(), target.getExpressionIndexes(), target.getExpressionIndexes());
            }
            if (compare != 0) {
                target.setMaxProbeMatch(target.getIterator().getCurrentIndex() - 1);
                return false;
            }
        }
        target.setMaxProbeMatch(target.getIterator().getCurrentIndex());
        return true;
    }

    protected int compare(List leftProbe,
                             List rightProbe,
                             int[] leftExpressionIndecies,
                             int[] rightExpressionIndecies) {
        for (int i = 0; i < leftExpressionIndecies.length; i++) {
            Object leftValue = leftProbe.get(leftExpressionIndecies[i]);
            Object rightValue = rightProbe.get(rightExpressionIndecies[i]);
            if (rightValue == null) {
                if (grouping && leftValue == null) {
                    continue;
                }
                return -1;
            }

            if (leftValue == null) {
                return 1;
            }

            int c = ((Comparable)rightValue).compareTo(leftValue);
            if (c != 0) {
                return c;
            }
        }

        // Found no difference, must be a match
        return 0;
    }
    
    /** 
     * @see com.metamatrix.query.processor.relational.JoinStrategy#loadLeft()
     */
    @Override
    protected void loadLeft() throws MetaMatrixComponentException,
                             MetaMatrixProcessingException {
        if (sortLeft != SortOption.SKIP_SORT) { 
            if (this.leftSort == null) {
                List sourceElements = joinNode.getChildren()[0].getElements();
                List expressions = this.joinNode.getLeftExpressions();
                this.leftSort = new SortUtility(this.leftSource.collectTuples(), sourceElements,
                                                    expressions, Collections.nCopies(expressions.size(), OrderBy.ASC), sortLeft == SortOption.SORT_DISTINCT,
                                                    this.joinNode.getBufferManager(), this.joinNode.getConnectionID());         
                this.leftSource.setDistinct(sortLeft == SortOption.SORT_DISTINCT);
            }
            this.leftSource.setTupleSource(leftSort.sort());
        } else if (this.joinNode.isDependent() || JoinType.JOIN_FULL_OUTER.equals(joinNode.getJoinType())) {
            super.loadLeft(); //buffer only for dependent and full outer joins
        }
    }
    
    /** 
     * @see com.metamatrix.query.processor.relational.JoinStrategy#loadRight()
     */
    @Override
    protected void loadRight() throws MetaMatrixComponentException,
                              MetaMatrixProcessingException {
        super.loadRight();
        if (processingSortRight != SortOption.SKIP_SORT) { 
            if (this.rightSort == null) {
                List sourceElements = joinNode.getChildren()[1].getElements();
                List expressions = this.joinNode.getRightExpressions();
                this.rightSort = new SortUtility(this.rightSource.getTupleSourceID(), sourceElements,
                                                    expressions, Collections.nCopies(expressions.size(), OrderBy.ASC), processingSortRight == SortOption.SORT_DISTINCT,
                                                    this.joinNode.getBufferManager(), this.joinNode.getConnectionID());
                this.rightSource.setDistinct(processingSortRight == SortOption.SORT_DISTINCT);
            }
            this.rightSource.setTupleSource(rightSort.sort());
        } 
    }
    
    public void setProcessingSortRight(boolean processingSortRight) {
        this.processingSortRight = processingSortRight?SortOption.SORT:SortOption.SKIP_SORT;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "MERGE JOIN"; //$NON-NLS-1$
    }

}