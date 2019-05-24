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

import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.Assertion;
import org.teiid.query.QueryPlugin;
import org.teiid.query.processor.relational.SourceState.ImplicitBuffer;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.Constant;


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
        ALREADY_SORTED, SORT, SORT_DISTINCT, NOT_SORTED
    }

    protected SortOption sortLeft;
    protected SortOption sortRight;

    /** false if three-level comparison, true if grouping comparison (null == null) */
    private boolean grouping;
    /** false if default processing, true if only a single outer match is allowed */
    protected boolean singleMatch;

    //load time state
    protected SortOption processingSortLeft;
    protected SortOption processingSortRight;

    public MergeJoinStrategy(SortOption sortLeft, SortOption sortRight, boolean grouping) {
        if (sortLeft == null) {
            sortLeft = SortOption.ALREADY_SORTED;
        }
        if (sortRight == null) {
            sortRight = SortOption.ALREADY_SORTED;
        }
        this.sortLeft = sortLeft;
        this.sortRight = sortRight;
        this.grouping = grouping;
    }

    /**
     * @see org.teiid.query.processor.relational.JoinStrategy#clone()
     */
    @Override
    public MergeJoinStrategy clone() {
        return new MergeJoinStrategy(sortLeft, sortRight, grouping).singleMatch(singleMatch);
    }

    /**
     * @see org.teiid.query.processor.relational.JoinStrategy#initialize(org.teiid.query.processor.relational.JoinNode)
     */
    @Override
    public void initialize(JoinNode joinNode) {
        super.initialize(joinNode);
        resetMatchState();
        this.processingSortRight = this.sortRight;
        this.processingSortLeft = this.sortLeft;
    }

    protected void resetMatchState() {
        this.outerState = this.leftSource;
        this.innerState = this.rightSource;
        this.mergeState = MergeState.SCAN;
        this.matchState = MatchState.MATCH_LEFT;
        this.loopState = LoopState.EVALUATE_CRITERIA;
        this.leftScanState = ScanState.READ;
        this.rightScanState = ScanState.READ;
        this.outerMatched = false;
    }

    /**
     * @see org.teiid.query.processor.relational.JoinStrategy#close()
     */
    @Override
    public void close() {
        super.close();
        this.outerState = null;
        this.innerState = null;
    }

    @Override
    protected void process() throws TeiidComponentException,
            TeiidProcessingException {
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
                            return;
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
                            return;
                        }
                    }
                }

                int result = 0;

                if (this.leftScanState == ScanState.DONE) {
                    if (this.rightScanState == ScanState.DONE) {
                        this.mergeState = MergeState.DONE;
                        return;
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
                        this.joinNode.addBatchRow(outputTuple(this.leftSource.getCurrentTuple(), this.rightSource.getOuterVals()));
                    }
                } else {
                    this.rightScanState = ScanState.READ;
                    if (joinNode.getJoinType() == JoinType.JOIN_FULL_OUTER) {
                        this.joinNode.addBatchRow(outputTuple(this.leftSource.getOuterVals(), this.rightSource.getCurrentTuple()));
                    }
                }
            }

            while (this.mergeState == MergeState.MATCH) {
                if (loopState == LoopState.LOAD_OUTER) {
                    if (compareToPrevious(outerState)) {
                        outerMatched = false;
                        innerState.reset();
                        loopState = LoopState.LOAD_INNER;
                    } else if (matchState == MatchState.MATCH_LEFT && joinNode.getJoinType() == JoinType.JOIN_FULL_OUTER && this.joinNode.getJoinCriteria() != null) {
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
                        this.leftSource.setMaxProbePosition();
                        this.rightSource.setMaxProbePosition();
                        this.mergeState = MergeState.SCAN;
                        this.matchState = MatchState.MATCH_LEFT;
                        break;
                    }
                }

                while (loopState == LoopState.LOAD_INNER || loopState == LoopState.EVALUATE_CRITERIA) {
                    if (loopState == LoopState.LOAD_INNER) {
                        if (!compareToPrevious(innerState)) {
                            loopState = LoopState.LOAD_OUTER;
                            break;
                        }
                        loopState = LoopState.EVALUATE_CRITERIA;
                    }

                    if (loopState == LoopState.EVALUATE_CRITERIA) {
                        List outputTuple = outputTuple(this.leftSource.getCurrentTuple(), this.rightSource.getCurrentTuple());

                        boolean match = this.joinNode.matchesCriteria(outputTuple);
                        loopState = LoopState.LOAD_INNER; // now that the criteria has been evaluated, try the next inner value

                        if (match) {
                            boolean wasMatched = outerMatched;
                            outerMatched = true;
                            if (matchState == MatchState.MATCH_LEFT && this.joinNode.getJoinType() != JoinType.JOIN_ANTI_SEMI) {
                                if (this.joinNode.getJoinType() == JoinType.JOIN_SEMI) {
                                    this.loopState = LoopState.LOAD_OUTER; //only one match is needed for semi join
                                }
                                this.joinNode.addBatchRow(outputTuple);
                                if (singleMatch && wasMatched) {
                                    throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID31293, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31293));
                                }
                                continue;
                            }
                            //right outer join || anti semi join can skip to the next outer value
                            this.loopState = LoopState.LOAD_OUTER;
                            break;
                        }
                    }
                }

                if (!outerMatched) {
                    if (matchState == MatchState.MATCH_RIGHT) {
                        this.joinNode.addBatchRow(outputTuple(this.leftSource.getOuterVals(), this.rightSource.getCurrentTuple()));
                    } else if (this.joinNode.getJoinType().isOuter()) {
                        this.joinNode.addBatchRow(outputTuple(this.leftSource.getCurrentTuple(), this.rightSource.getOuterVals()));
                    }
                }
            }
        }
    }

    protected boolean compareToPrevious(SourceState target) throws TeiidComponentException,
                                    TeiidProcessingException {
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
            if (!target.isExpresssionDistinct()) {
                compare = compare(previousTuple, target.getCurrentTuple(), target.getExpressionIndexes(), target.getExpressionIndexes());
                if (compare < 0) {
                    boolean ignore = false;
                    for (int i : target.getExpressionIndexes()) {
                        if (target.getCurrentTuple().get(i) == null) {
                            //since we don't specify a null sort option, it could be returned as high/last
                            ignore = true;
                            break;
                        }
                    }
                    if (!ignore) {
                        //sanity check - this means the sort order is not as expected
                        //note this is not a complete check - it will not detect all invalid circumstances as we exit early
                        throw new TeiidComponentException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31202));
                    }
                }
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
        return compareTuples(leftProbe, rightProbe, leftExpressionIndecies,
                rightExpressionIndecies, grouping, false);
    }

    /**
     * 0 if the values match
     * positive if right is greater than left
     * negative if left is greater than right
     *
     * In column diff mode, return -1 if the they are the same
     * else return the index where they differ
     *
     * @param leftProbe
     * @param rightProbe
     * @param leftExpressionIndecies
     * @param rightExpressionIndecies
     * @param nullEquals
     * @param columnDiff
     * @return
     */
    public static int compareTuples(List<?> leftProbe, List<?> rightProbe,
            int[] leftExpressionIndecies, int[] rightExpressionIndecies, boolean nullEquals, boolean columnDiff) {
        for (int i = 0; i < leftExpressionIndecies.length; i++) {
            Object leftValue = leftProbe.get(leftExpressionIndecies[i]);
            Object rightValue = rightProbe.get(rightExpressionIndecies[i]);
            if (rightValue == null) {
                if (nullEquals && leftValue == null) {
                    continue;
                }
                if (columnDiff) {
                    return i;
                }
                return -1;
            }

            if (leftValue == null) {
                if (columnDiff) {
                    return i;
                }
                return 1;
            }

            int c = Constant.COMPARATOR.compare(rightValue, leftValue);
            if (c != 0) {
                if (columnDiff) {
                    return i;
                }
                return c;
            }
        }
        if (columnDiff) {
            return -1;
        }
        // Found no difference, must be a match
        return 0;
    }

    @Override
    protected void loadLeft() throws TeiidComponentException,
            TeiidProcessingException {
        this.leftSource.sort(this.processingSortLeft);
    }

    @Override
    protected void loadRight() throws TeiidComponentException,
            TeiidProcessingException {
        if (this.joinNode.getJoinType() != JoinType.JOIN_FULL_OUTER || this.joinNode.getJoinCriteria() == null) {
            this.rightSource.setImplicitBuffer(ImplicitBuffer.ON_MARK);
        }
        this.rightSource.sort(this.processingSortRight);
    }

    public void setProcessingSortRight(boolean processingSortRight) {
        if (processingSortRight && this.processingSortRight == SortOption.ALREADY_SORTED) {
            //it is possible that a delayed open will be called after the parent open
            //for now we'll just throw an assertion
            Assertion.assertTrue(!this.rightSource.open);
            this.processingSortRight = SortOption.SORT;
        }
    }

    public void setProcessingSortLeft(boolean processingSortLeft) {
        if (processingSortLeft && this.processingSortLeft == SortOption.ALREADY_SORTED) {
            //it is possible that a delayed open will be called after the parent open
            //for now we'll just throw an assertion
            Assertion.assertTrue(!this.leftSource.open);
            this.processingSortLeft = SortOption.SORT;
        }
    }

    public String getName() {
        return "MERGE JOIN"; //$NON-NLS-1$
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getName())
                .append(" (").append(sortLeft).append("/").append(sortRight).append(")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        if (singleMatch) {
            sb.append(" subquery"); //$NON-NLS-1$
        }
        return sb.toString();
    }

    /**
     * Optional flag for a left outer join to enforce the single
     * row restriction of a scalar subquery
     */
    public MergeJoinStrategy singleMatch(boolean b) {
        this.singleMatch = b;
        return this;
    }

}