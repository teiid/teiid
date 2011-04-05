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

package org.teiid.query.optimizer.relational.rules;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.AbstractSetCriteria;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.PredicateCriteria;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;

/** 
 * @since 4.3
 */
public class NewCalculateCostUtil {

    public static final int UNKNOWN_JOIN_SCALING = 20;
	public static final float UNKNOWN_VALUE = -1;
    
    // the following variables are used to hold cost estimates (roughly in milliseconds)
    private final static float compareTime = .0001f; //TODO: a better estimate would be based upon the number of conjuncts
    private final static float readTime = .001f; //TODO: should come from the connector
    private final static float procNewRequestTime = 1; //TODO: should come from the connector
    
    enum Stat {
    	NDV,
    	NNV
    }
    
    public static class DependentCostAnalysis {
    	Float[] maxNdv;
    	Float[] expectedNdv;
    	Float expectedCardinality;
    }

    @SuppressWarnings("serial")
	private static class ColStats extends LinkedHashMap<Expression, float[]> {
    	@Override
    	public String toString() {
    		StringBuilder sb = new StringBuilder();
    		sb.append('{');
    		
    		int j = 0;
    		for (Iterator<Entry<Expression, float[]>> i = this.entrySet().iterator(); i.hasNext();) {
    		    Entry<Expression, float[]> e = i.next();
    		    sb.append(e.getKey());
    		    sb.append('=');
    		    sb.append(Arrays.toString(e.getValue()));
    		    j++;
    		    if (i.hasNext()) {
    		    	sb.append(", "); //$NON-NLS-1$
    		    	if (j > 3) {
        		    	sb.append("..."); //$NON-NLS-1$
        		    	break;
        		    }
    		    }
    		}
    		return sb.append('}').toString();
    	}
    }
        
    /**
     * Calculate cost of a node and all children, recursively from the bottom up.
     * @param node
     * @param metadata
     * @return Cost computed at the passed node
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    static float computeCostForTree(PlanNode node, QueryMetadataInterface metadata) 
        throws QueryMetadataException, TeiidComponentException {

    	updateCardinality(node, metadata);
        
        return node.getCardinality();
    }
    
    static boolean updateCardinality(PlanNode node, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
    	Float cost = (Float) node.getProperty(NodeConstants.Info.EST_CARDINALITY);

    	// check if already computed
    	boolean updated = false;
    	for (PlanNode child : node.getChildren()) {
    		updated |= updateCardinality(child, metadata);
        }
        if(cost == null || updated) {
            computeNodeCost(node, metadata);
            return true;
        }
        return false;
    }
   
    /**
     * This method attempts to estimate a cost for each type of node.
     * @param node
     * @param metadata
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private static void computeNodeCost(PlanNode node, QueryMetadataInterface metadata) 
        throws QueryMetadataException, TeiidComponentException {
        
        switch(node.getType()) {
            case NodeConstants.Types.SOURCE:
                estimateSourceNodeCost(node, metadata);
                break;

            case NodeConstants.Types.SELECT:
                estimateSelectNodeCost(node, metadata);
                break;
                           
            case NodeConstants.Types.JOIN:
                estimateJoinNodeCost(node, metadata);
                break;

            case NodeConstants.Types.DUP_REMOVE:
                estimateNodeCost(node, FrameUtil.findTopCols(node), metadata);
                break;

            case NodeConstants.Types.GROUP:
            	if (!node.hasCollectionProperty(NodeConstants.Info.GROUP_COLS)) {
            		setCardinalityEstimate(node, 1f, true, metadata);
            	} else {
            		estimateNodeCost(node, (List)node.getProperty(NodeConstants.Info.GROUP_COLS), metadata);
            	}
                break;
            case NodeConstants.Types.ACCESS:
            case NodeConstants.Types.SORT:
            {
                //Simply record the cost of the only child
                PlanNode child = node.getFirstChild();
                Float childCost = (Float)child.getProperty(NodeConstants.Info.EST_CARDINALITY);
                setCardinalityEstimate(node, childCost, true, metadata);
                break;
            }
            case NodeConstants.Types.NULL:
                setCardinalityEstimate(node, 0f, true, metadata);
                break;

            case NodeConstants.Types.PROJECT:
            {
                Float childCost = null;
                //Simply record the cost of the only child
                if (node.getChildCount() != 0) {
                    PlanNode child = node.getFirstChild();
                    childCost = (Float)child.getProperty(NodeConstants.Info.EST_CARDINALITY);
                } else {
                    childCost = 1f;
                }
                setCardinalityEstimate(node, childCost, true, metadata);
                break;
            }
            case NodeConstants.Types.SET_OP: 
            {
                estimateSetOpCost(node, metadata);
                break;
            }
            case NodeConstants.Types.TUPLE_LIMIT: 
            {
                PlanNode child = node.getFirstChild();
                float childCost = child.getCardinality();
                
                Expression offset = (Expression)node.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
                Float cost = childCost;
                
                if (childCost != UNKNOWN_VALUE && offset instanceof Constant) {
                    float offsetCost = childCost - ((Number)((Constant)offset).getValue()).floatValue();
                    cost = new Float((offsetCost < 0) ? 0 : offsetCost);
                }
                
                Expression limit = (Expression)node.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
                if (limit instanceof Constant) {
                    float limitCost = ((Number)((Constant)limit).getValue()).floatValue();
                    if (cost.floatValue() != UNKNOWN_VALUE) {
                        cost = new Float(Math.min(limitCost, cost.floatValue()));
                    } else {
                        cost = new Float(limitCost);
                    }
                } 
                setCardinalityEstimate(node, cost, true, metadata);
                break;
            }
        }
    }

	private static void estimateSetOpCost(PlanNode node,
			QueryMetadataInterface metadata) throws QueryMetadataException,
			TeiidComponentException {
		float cost = 0;
		
		SetQuery.Operation op = (SetQuery.Operation)node.getProperty(NodeConstants.Info.SET_OPERATION);

		float leftCost = (Float)node.getFirstChild().getProperty(NodeConstants.Info.EST_CARDINALITY);
		float rightCost = (Float)node.getLastChild().getProperty(NodeConstants.Info.EST_CARDINALITY);
		
		if (!node.hasBooleanProperty(NodeConstants.Info.USE_ALL)) {
			leftCost = getDistinctEstimate(node.getFirstChild(), metadata, leftCost);
			rightCost = getDistinctEstimate(node.getLastChild(), metadata, rightCost);
		}
		
		cost = getCombinedSetEstimate(op, leftCost, rightCost, !node.hasBooleanProperty(NodeConstants.Info.USE_ALL));
		
		setCardinalityEstimate(node, new Float(cost), true, metadata);
	}

	private static float getCombinedSetEstimate(SetQuery.Operation op, float leftCost, float rightCost, boolean distinct) {
		float cost;
		cost = leftCost;
		
		switch (op) {
		case EXCEPT:
			if (leftCost != UNKNOWN_VALUE && rightCost != UNKNOWN_VALUE) {
		    	cost = Math.max(1, leftCost - .5f * rightCost);
		    }
			break;
		case INTERSECT:
			if (rightCost != UNKNOWN_VALUE) {
				if (leftCost != UNKNOWN_VALUE) {
		    		cost = .5f * Math.min(leftCost, rightCost);
				} else {
					cost = rightCost;
				}
			}
			break;
		default: //union
			if (leftCost != UNKNOWN_VALUE && rightCost != UNKNOWN_VALUE) {
		        if (distinct) {
		        	cost = Math.max(leftCost, rightCost) + .5f * Math.min(leftCost, rightCost);
		        } else {
		            cost = rightCost + leftCost;
		        }
			}
			break;
		}
		return cost;
	}

	private static float getDistinctEstimate(PlanNode node,
			QueryMetadataInterface metadata, float cost)
			throws QueryMetadataException, TeiidComponentException {
		PlanNode projectNode = NodeEditor.findNodePreOrder(node, NodeConstants.Types.PROJECT);
		if (projectNode != null) {
			cost = getNDVEstimate(node.getParent(), metadata, cost, (List)projectNode.getProperty(NodeConstants.Info.PROJECT_COLS), false);
		}
		return cost;
	}

    private static void setCardinalityEstimate(PlanNode node, Float bestEstimate, boolean setColEstimates, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        if (bestEstimate == null){
        	bestEstimate = Float.valueOf(UNKNOWN_VALUE);
        }
        Float lastEstimate = (Float)node.setProperty(NodeConstants.Info.EST_CARDINALITY, bestEstimate);
        if (node.getParent() != null && (lastEstimate == null || !lastEstimate.equals(bestEstimate))) {
        	node.getParent().setProperty(Info.EST_CARDINALITY, null);
        }
        if (setColEstimates) {
        	setColStatEstimates(node, bestEstimate, metadata);
        }
    }

    /**
     * Method estimateJoinNodeCost.
     * @param node
     * @param metadata
     */
    private static void estimateJoinNodeCost(PlanNode node, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {

        Iterator<PlanNode> children = node.getChildren().iterator();
        PlanNode child1 = children.next();
        float childCost1 = child1.getCardinality();
        PlanNode child2 = children.next();
        float childCost2 = child2.getCardinality();
        
        if (childCost1 == UNKNOWN_VALUE || childCost2 == UNKNOWN_VALUE) {
        	setCardinalityEstimate(node, null, true, metadata);
        	return;
        }

        JoinType joinType = (JoinType)node.getProperty(NodeConstants.Info.JOIN_TYPE);
        List joinCriteria = (List) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);
        
        float baseCost = childCost1 * childCost2;

        if (joinCriteria != null && !joinCriteria.isEmpty()) {
    		Criteria crit = Criteria.combineCriteria(joinCriteria);
    		//TODO: we may be able to get a fairly accurate join estimate if the
    		//unknown side is being joined with a key
        	baseCost = recursiveEstimateCostOfCriteria(baseCost, node, crit, metadata);
        }
        
        Float cost = null;
        if (JoinType.JOIN_CROSS.equals(joinType) || JoinType.JOIN_INNER.equals(joinType)){
            cost = baseCost;
        } else if (JoinType.JOIN_FULL_OUTER.equals(joinType)) {
            cost = Math.max((childCost1+childCost2),baseCost);
        } else if (JoinType.JOIN_LEFT_OUTER.equals(joinType)) {
            cost = Math.max(childCost1,baseCost);
        } else if (JoinType.JOIN_SEMI.equals(joinType) || JoinType.JOIN_ANTI_SEMI.equals(joinType)) {
        	cost = Math.min(childCost1, baseCost);
        }
        
        setCardinalityEstimate(node, cost, true, metadata);
    }

    /**
     * Estimate the cost of a selection.  This is not easy to do without information 
     * about the value count for each relation attribute.  
     * @param metadata
     */
    private static void estimateSelectNodeCost(PlanNode node, QueryMetadataInterface metadata) 
        throws QueryMetadataException, TeiidComponentException {

        PlanNode child = node.getFirstChild();
        float childCost = child.getCardinality();
        
        //Get list of conjuncts
        Criteria selectCriteria = (Criteria)node.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        float newCost = recursiveEstimateCostOfCriteria(childCost, node, selectCriteria, metadata);
        setCardinalityEstimate(node, newCost, true, metadata);
    }
    
    private static void setColStatEstimates(PlanNode node, float cardinality, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
    	if (cardinality == UNKNOWN_VALUE) {
    		return;
    	}
    	ColStats colStats = null;
    	ColStats colStatsOther = null;
    	float childCardinality = UNKNOWN_VALUE;
    	if (node.getChildCount() > 0) {
    		childCardinality = node.getFirstChild().getCardinality();
    		colStats = (ColStats) node.getFirstChild().getProperty(Info.EST_COL_STATS);
    	} 
    	float otherChildCardinality = UNKNOWN_VALUE;
    	List<? extends Expression> outputColsOther = null;
    	if (node.getChildCount() > 1) {
    		otherChildCardinality = node.getLastChild().getCardinality();
    		colStatsOther = (ColStats) node.getLastChild().getProperty(Info.EST_COL_STATS);
    		outputColsOther = getOutputCols(node.getLastChild(), metadata);
    	}
    	SetQuery.Operation setOp = (Operation) node.getProperty(Info.SET_OPERATION);
    	List<? extends Expression> outputCols = getOutputCols(node, metadata);
    	ColStats newColStats = new ColStats();
    	for (int i = 0; i < outputCols.size(); i++) {
    		Expression expr = outputCols.get(i);
    		float[] newStats = new float[2];
    		Arrays.fill(newStats, UNKNOWN_VALUE);
    		if (childCardinality == UNKNOWN_VALUE || (setOp != null && (colStats == null || colStatsOther == null))) {
    			//base case - cannot determine, just assume unique rows
        		newStats[Stat.NDV.ordinal()] = cardinality;
        		newStats[Stat.NNV.ordinal()] = 0;
    		} else if (setOp != null) {
    			//set op
				float[] stats = colStats.get(expr);
				float[] statsOther = colStatsOther.get(outputColsOther.get(i));
				newStats[Stat.NDV.ordinal()] = getCombinedSetEstimate(setOp, stats[Stat.NDV.ordinal()], statsOther[Stat.NDV.ordinal()], true);
        		newStats[Stat.NNV.ordinal()] = getCombinedSetEstimate(setOp, stats[Stat.NNV.ordinal()], statsOther[Stat.NNV.ordinal()], !node.hasBooleanProperty(NodeConstants.Info.USE_ALL));
    		} else {
    			//all other cases - join is the only multi-node case here
    			float[] stats = null;
    			float origCardinality = childCardinality;
    			if (colStats != null) {
    				stats = colStats.get(expr);
    			}
    			if (stats == null && colStatsOther != null) {
    				origCardinality = otherChildCardinality;
    				stats = colStatsOther.get(expr);
    			}
    			if (stats == null) {
        			if (node.getType() == NodeConstants.Types.PROJECT) {
	        			Collection<SingleElementSymbol> elems = new HashSet<SingleElementSymbol>();
	        			AggregateSymbolCollectorVisitor.getAggregates(expr, elems, elems);
	        			newStats[Stat.NDV.ordinal()] = getStat(Stat.NDV, elems, node, childCardinality, metadata);
		        		newStats[Stat.NNV.ordinal()] = getStat(Stat.NNV, elems, node, childCardinality, metadata);
        			} else {
        				//TODO: use a better estimate for new aggs
        				if (node.hasProperty(Info.GROUP_COLS)) {
            				newStats[Stat.NDV.ordinal()] = cardinality / 3;
        				} else {
        					newStats[Stat.NDV.ordinal()] = cardinality;
        				}
                		newStats[Stat.NNV.ordinal()] = UNKNOWN_VALUE;
        			}
        		} else {
        			if (node.getType() == NodeConstants.Types.DUP_REMOVE || node.getType() == NodeConstants.Types.GROUP) {
        				//don't scale down
        				newStats[Stat.NDV.ordinal()] = stats[Stat.NDV.ordinal()];
        			} else if (stats[Stat.NDV.ordinal()] != UNKNOWN_VALUE) {
    					newStats[Stat.NDV.ordinal()] = stats[Stat.NDV.ordinal()]*Math.min(1, cardinality/origCardinality);
    					newStats[Stat.NDV.ordinal()] = Math.max(1, newStats[Stat.NDV.ordinal()]);
        			}
    				if (stats[Stat.NNV.ordinal()] != UNKNOWN_VALUE) {
    					//TODO: this is an under estimate for the inner side of outer joins
	        			newStats[Stat.NNV.ordinal()] = stats[Stat.NNV.ordinal()]*Math.min(1, cardinality/origCardinality);
	        			newStats[Stat.NNV.ordinal()] = Math.max(1, newStats[Stat.NNV.ordinal()]);
    				}
        		}
    		}
    		newColStats.put(expr, newStats);
		}
    	node.setProperty(Info.EST_COL_STATS, newColStats);
    }

    /**
     * For a source node, the cost is basically the cardinality of the source
     * (if it is known).
     * @param node
     * @param metadata
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private static void estimateSourceNodeCost(PlanNode node, QueryMetadataInterface metadata) 
        throws QueryMetadataException, TeiidComponentException {
        
        float cost = UNKNOWN_VALUE;
        if(node.getChildCount() > 0) {
        	SymbolMap references = (SymbolMap)node.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
        	//only cost non-correlated TODO: a better estimate for correlated
        	if (references == null) {
	            PlanNode child = node.getFirstChild();
	            cost = child.getCardinality();
	            SymbolMap symbolMap = (SymbolMap)node.getProperty(NodeConstants.Info.SYMBOL_MAP);
	            if (symbolMap != null) {
		            ColStats colStats = (ColStats) child.getProperty(Info.EST_COL_STATS);
		            if (colStats != null) {
		            	List<? extends Expression> outputCols = getOutputCols(node, metadata);
		            	ColStats newColStats = new ColStats();
		            	for (Expression expr : outputCols) {
		            		if (!(expr instanceof ElementSymbol)) {
		            			continue;
		            		}
		            		ElementSymbol es = (ElementSymbol)expr;
		            		Expression ex = symbolMap.getMappedExpression(es);
							newColStats.put(es, colStats.get(ex));
						}
		        		node.setProperty(Info.EST_COL_STATS, newColStats);
		            } else {
		            	colStats = createColStats(node, metadata, cost);
		        		node.setProperty(Info.EST_COL_STATS, colStats);
		            }
	            }
        	}
        }else {
            GroupSymbol group = node.getGroups().iterator().next();
            float cardinality = metadata.getCardinality(group.getMetadataID());
            if (cardinality <= QueryMetadataInterface.UNKNOWN_CARDINALITY){
                cardinality = UNKNOWN_VALUE;
            }
            cost = cardinality;
            if (!node.hasProperty(Info.ATOMIC_REQUEST)) {
	            ColStats colStats = createColStats(node, metadata, cost);
	    		node.setProperty(Info.EST_COL_STATS, colStats);
            }
        }
        
        setCardinalityEstimate(node, new Float(cost), false, metadata);
    }

	private static ColStats createColStats(PlanNode node,
			QueryMetadataInterface metadata, float cardinality)
			throws QueryMetadataException, TeiidComponentException {
		ColStats colStats = new ColStats();
		List<? extends Expression> outputCols = getOutputCols(node, metadata);
		for (Expression expr : outputCols) {
			if (!(expr instanceof ElementSymbol)) {
				continue;
			}
			ElementSymbol es = (ElementSymbol)expr;
			float[] vals = new float[2];
			float ndv = metadata.getDistinctValues(es.getMetadataID());
			float nnv = metadata.getNullValues(es.getMetadataID());
			if (cardinality != UNKNOWN_VALUE) {
            	int groupCardinality = metadata.getCardinality(es.getGroupSymbol().getMetadataID());
            	if (groupCardinality != UNKNOWN_VALUE && groupCardinality > cardinality) {
            		if (ndv != UNKNOWN_VALUE) {
            			ndv *= cardinality / Math.max(1, groupCardinality);
            			ndv = Math.max(ndv, 1);
            		}
            		if (nnv != UNKNOWN_VALUE) {
            			nnv *= cardinality / Math.max(1, groupCardinality);
            			nnv = Math.max(nnv, 1);
            		}
            	}
			}
			vals[Stat.NDV.ordinal()] = ndv;
			vals[Stat.NNV.ordinal()] = nnv;
			colStats.put(es, vals);
		}
		return colStats;
	}

	private static List<? extends Expression> getOutputCols(PlanNode node,
			QueryMetadataInterface metadata) throws QueryMetadataException,
			TeiidComponentException {
		List<Expression> outputCols =(List<Expression>)node.getProperty(Info.OUTPUT_COLS);
		if (outputCols != null) {
			return outputCols;
		}
		PlanNode projectNode = NodeEditor.findNodePreOrder(node, 
				NodeConstants.Types.PROJECT | NodeConstants.Types.GROUP 
				| NodeConstants.Types.SOURCE | NodeConstants.Types.JOIN 
				| NodeConstants.Types.NULL);
		if (projectNode != null) {
			node = projectNode;
		} 
		
		if (node.getType() == NodeConstants.Types.PROJECT) {
			return (List<? extends Expression>) node.getProperty(NodeConstants.Info.PROJECT_COLS);
		} else if (node.getType() == NodeConstants.Types.GROUP) {
			LinkedList<Expression> result = new LinkedList<Expression>(RulePushAggregates.collectAggregates(node));
			result.addAll((Collection<? extends Expression>) node.getProperty(Info.GROUP_COLS));
			return result;
		} 
		LinkedList<ElementSymbol> elements = new LinkedList<ElementSymbol>();
		for (GroupSymbol group : node.getGroups()) {
			elements.addAll(ResolverUtil.resolveElementsInGroup(group, metadata));
		}
		return elements;
	} 
    
    /**
     * For a Group or Dup Removal node, the cost is basically the smaller of the largest NDV of the 
     * selected columns and cost of the child node (if it is known).
     * @param node
     * @param metadata
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private static void estimateNodeCost(PlanNode node, List expressions, QueryMetadataInterface metadata) 
        throws QueryMetadataException, TeiidComponentException {
        
        PlanNode child = node.getFirstChild();
        float childCost = child.getCardinality();
        
        if(childCost == UNKNOWN_VALUE) {
            setCardinalityEstimate(node, null, true, metadata);
            return;
        }

        float cardinality = getNDVEstimate(node, metadata, childCost, expressions, true);
        setCardinalityEstimate(node, cardinality, true, metadata);
    }

    static float getStat(Stat stat, Collection<? extends Expression> elems, PlanNode node,
    		float cardinality, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        float result = 1;
        int branch = 0;
        boolean branchFound = false;
        for (Expression expression : elems) {
        	ColStats colStats = null;
	    	if (node.getChildCount() == 0) {
	    		colStats = createColStats(node, metadata, cardinality);
	    	} else {
		    	for (int i = branch; i < node.getChildCount(); i++) {
		    		PlanNode child = node.getChildren().get(i);
			    	colStats = (ColStats) child.getProperty(Info.EST_COL_STATS);
			        if (colStats == null) {
			        	continue;
			        }
					float[] stats = colStats.get(expression);
					if (stats != null) {
						if (node.getType() == NodeConstants.Types.SET_OP) {
							branch = i;
							branchFound = true;
						}
						break;
					}
					colStats = null;
					if (branchFound) {
						break;
					}
		    	}
	    	}
	    	if (colStats == null) {
	    		return UNKNOWN_VALUE;
	    	}
	    	float[] stats = colStats.get(expression);
			if (stats == null || stats[stat.ordinal()] == UNKNOWN_VALUE) {
				return UNKNOWN_VALUE;
			}
			result = Math.max(result, stats[stat.ordinal()]);
    	}
		return result;
	}

	static float recursiveEstimateCostOfCriteria(float childCost, PlanNode currentNode, Criteria crit, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {
        
        float cost = childCost; 
        if(crit instanceof CompoundCriteria) {
            CompoundCriteria compCrit = (CompoundCriteria) crit;
            if (compCrit.getOperator() == CompoundCriteria.OR) {
                cost = 0;
            } 
            if (usesKey(compCrit, metadata)) {
                return 1;
            }
            for (Criteria critPart : compCrit.getCriteria()) {
                float nextCost = recursiveEstimateCostOfCriteria(childCost, currentNode, critPart, metadata);
                
                if(compCrit.getOperator() == CompoundCriteria.AND) {
                    if (nextCost == UNKNOWN_VALUE) {
                        continue;
                    }
                    if (childCost != UNKNOWN_VALUE) {
                        cost *= nextCost/childCost;
                    } else {
                        if (cost == UNKNOWN_VALUE) {
                            cost = nextCost;
                        } else {
                            cost = Math.min(cost, nextCost);
                        }
                    }
                    if (cost <= 1) {
                        return 1;
                    }
                } else {
                    if (nextCost == UNKNOWN_VALUE) {
                        return childCost;
                    } 
                    //this assumes that all disjuncts are completely disjoint
                    cost += nextCost;
                    if (childCost != UNKNOWN_VALUE) {
                        cost = Math.min(cost, childCost);
                    }
                }
            }
            if (cost == UNKNOWN_VALUE) {
                return childCost;
            }
        } else if(crit instanceof NotCriteria){
            if (childCost == UNKNOWN_VALUE) {
                return UNKNOWN_VALUE;
            }
            float nextCost = recursiveEstimateCostOfCriteria(childCost, currentNode, ((NotCriteria)crit).getCriteria(), metadata);
            if (nextCost == UNKNOWN_VALUE){
                return childCost;
            }   
            cost -= nextCost;
        } else {
            cost = estimatePredicateCost(childCost, currentNode, (PredicateCriteria) crit, metadata);
            
            if (cost == UNKNOWN_VALUE) {
                return childCost;
            }
        }  
        
        cost = Math.max(cost, 1);
        
        return cost;
    }
    
    /** 
     * This method is a helper to examine whether a compound criteria covers
     * a compound key.  A "valid" criteria is 
     * 1) a predicate criteria
     *   1a) not negated
     *   1b) with an equality operator if it is a compare criteria
     * b) or a compound criteria containing valid criteria and an "AND" operator 
     * @param criteria
     * @param elements Collection to collect ElementSymbols in
     * @since 4.2
     */
    private static void collectElementsOfValidCriteria(Criteria criteria, Collection<ElementSymbol> elements) {
       
        if(criteria instanceof CompoundCriteria) {
            CompoundCriteria compCrit = (CompoundCriteria) criteria;
            Iterator<Criteria> iter = compCrit.getCriteria().iterator();
            boolean first = true;
            Collection<ElementSymbol> savedElements = elements;
            if(compCrit.getOperator() == CompoundCriteria.OR) {
            	elements = new HashSet<ElementSymbol>();
            }
            while(iter.hasNext()) { 
            	if(compCrit.getOperator() == CompoundCriteria.AND || first) {
            		collectElementsOfValidCriteria(iter.next(), elements);
            		first = false;
            	} else {
            		HashSet<ElementSymbol> other = new HashSet<ElementSymbol>();
            		collectElementsOfValidCriteria(iter.next(), other);
            		elements.retainAll(other);
            	}
            }
            if (compCrit.getOperator() == CompoundCriteria.OR) {
            	savedElements.addAll(elements);
            }
        } else if(criteria instanceof CompareCriteria) {
            CompareCriteria compCrit = (CompareCriteria)criteria;
            if (compCrit.getOperator() == CompareCriteria.EQ){
                ElementCollectorVisitor.getElements(compCrit, elements);
            }                 
        } else if(criteria instanceof MatchCriteria) {
            MatchCriteria matchCriteria = (MatchCriteria)criteria;
            if (!matchCriteria.isNegated()) {
                ElementCollectorVisitor.getElements(matchCriteria, elements);
            }

        } else if(criteria instanceof AbstractSetCriteria) {
            AbstractSetCriteria setCriteria = (AbstractSetCriteria)criteria;
            if (!setCriteria.isNegated()) {
                ElementCollectorVisitor.getElements(setCriteria.getExpression(), elements);
            }
        } else if(criteria instanceof IsNullCriteria) {
            IsNullCriteria isNullCriteria = (IsNullCriteria)criteria;
            if (!isNullCriteria.isNegated()) {
                ElementCollectorVisitor.getElements(isNullCriteria.getExpression(), elements);
            }
        }        
    }
    
    /** 
     * @param childCost
     * @param predicateCriteria
     * @param metadata
     * @return
     * @since 4.3
     */
    private static float estimatePredicateCost(float childCost, PlanNode currentNode, PredicateCriteria predicateCriteria, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {
        
        Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(predicateCriteria, true);
        
        Collection<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(elements);
        boolean multiGroup = groups.size() > 1;
        
        float cost = childCost;
        float ndv = getStat(Stat.NDV, elements, currentNode, childCost, metadata);
        
        boolean unknownChildCost = childCost == UNKNOWN_VALUE;
        boolean usesKey = usesKey(elements, metadata);
        
        if (childCost == UNKNOWN_VALUE) {
            childCost = 1;
        }

        if (ndv == UNKNOWN_VALUE) {
            if (multiGroup) {
                if (usesKey) {
                    ndv = (float)Math.ceil(Math.sqrt(childCost));
                } else {
                    ndv = (float)Math.ceil(Math.sqrt(childCost)/4);
                }
            } else if (usesKey) {
                ndv = childCost;
            } else {
                ndv = (float)Math.ceil(Math.sqrt(childCost)/2);
            }
            ndv = Math.max(ndv, 1);
        } 
                
        boolean isNegatedPredicateCriteria = false;
        if(predicateCriteria instanceof CompareCriteria) {
            CompareCriteria compCrit = (CompareCriteria) predicateCriteria;
                        
            if (compCrit.getOperator() == CompareCriteria.EQ || compCrit.getOperator() == CompareCriteria.NE){
                if (unknownChildCost && (!usesKey || multiGroup)) {
                    return UNKNOWN_VALUE;
                }
                cost = childCost / ndv;
                if (compCrit.getOperator() == CompareCriteria.NE) {
                    isNegatedPredicateCriteria = true;
                }
            } else { //GE, LE, GT, LT
                cost = getCostForComparison(childCost, metadata, compCrit, unknownChildCost);
            }
        } else if(predicateCriteria instanceof MatchCriteria) {
            MatchCriteria matchCriteria = (MatchCriteria)predicateCriteria;
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            cost = estimateMatchCost(childCost, ndv, matchCriteria);
            
            isNegatedPredicateCriteria = matchCriteria.isNegated();

        } else if(predicateCriteria instanceof SetCriteria) {
            SetCriteria setCriteria = (SetCriteria) predicateCriteria;
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            cost = childCost * setCriteria.getNumberOfValues() / ndv;
            
            isNegatedPredicateCriteria = setCriteria.isNegated();
            
        } else if(predicateCriteria instanceof SubquerySetCriteria) {
            SubquerySetCriteria setCriteria = (SubquerySetCriteria) predicateCriteria;
            
            // TODO - use inner ProcessorPlan cardinality estimates 
            // to determine the estimated number of values
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            cost = childCost / 3;
            
            isNegatedPredicateCriteria = setCriteria.isNegated();

        } else if(predicateCriteria instanceof IsNullCriteria) {
            IsNullCriteria isNullCriteria = (IsNullCriteria)predicateCriteria;

            float nnv = getStat(Stat.NNV, elements, currentNode, childCost, metadata);
            if (nnv == UNKNOWN_VALUE) {
            	if (unknownChildCost) {
            		return UNKNOWN_VALUE;
            	}
                cost = childCost / ndv;
            } else {
                cost = nnv;
            }
            
            isNegatedPredicateCriteria = isNullCriteria.isNegated();
        } else if (predicateCriteria instanceof DependentSetCriteria) {
        	DependentSetCriteria dsc = (DependentSetCriteria)predicateCriteria;
        	
        	if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
        	if (dsc.getNdv() == UNKNOWN_VALUE) {
        		return childCost / 3;
        	}
        	
        	cost = childCost * dsc.getNdv() / ndv;
        }

        if (cost == UNKNOWN_VALUE) {
            return UNKNOWN_VALUE;
        }
        
        if (cost > childCost) {
            cost = childCost;
        }
        
        if (isNegatedPredicateCriteria) {
            // estimate for NOT in the predicate
            cost = (cost != UNKNOWN_VALUE)
                    ? Math.max( childCost - cost, 1)
                    : UNKNOWN_VALUE;
        }
                
        return cost;
    }

    /** 
     * TODO: does not check for escape char
     * or if it will contain single match chars
     */
    private static float estimateMatchCost(float childCost,
                                           float ndv,
                                           MatchCriteria criteria) {
        Expression matchExpression = criteria.getRightExpression();
        if(matchExpression instanceof Constant && ((Constant)matchExpression).getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
            String compareValue = (String) ((Constant)matchExpression).getValue();
            if(compareValue != null && compareValue.indexOf('%') < 0) {
            	return (childCost / 2) * (1 / 3f  + 1 / ndv); //without knowing length constraints we'll make an average guess
            }
        } else if (EvaluatableVisitor.willBecomeConstant(criteria.getLeftExpression())) {
            return childCost / ndv;
        }
        return childCost / 3;
    }

    private static float getCostForComparison(float childCost,
                                              QueryMetadataInterface metadata,
                                              CompareCriteria compCrit, boolean unknownChildCost) throws TeiidComponentException,
                                                                       QueryMetadataException {
        if (!(compCrit.getLeftExpression() instanceof ElementSymbol) || !(compCrit.getRightExpression() instanceof Constant)) {
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            return childCost/3;
        }
        ElementSymbol element = (ElementSymbol)compCrit.getLeftExpression();
        Class<?> dataType = compCrit.getRightExpression().getType();
    
        String max = (String)metadata.getMaximumValue(element.getMetadataID());
        String min = (String)metadata.getMinimumValue(element.getMetadataID());
        if(max == null || min == null) {
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            return childCost/3;
        } 
        float cost = childCost;
        try{
            float maxValue = 0;
            float minValue = 0;

            Constant value = (Constant)compCrit.getRightExpression();
            float compareValue = 0;
        	// Case 6257 - handling added for time and date.  If the max/min values are not
            // in the expected format, NumberFormatException is thrown and reverts to default costing.
            if(dataType.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
                compareValue = ((Timestamp)value.getValue()).getTime();
                maxValue = Timestamp.valueOf(max).getTime();
                minValue = Timestamp.valueOf(min).getTime();
            } else if(dataType.equals(DataTypeManager.DefaultDataClasses.TIME)) {
                compareValue = ((Time)value.getValue()).getTime();
                maxValue = Time.valueOf(max).getTime();
                minValue = Time.valueOf(min).getTime();
            // (For date, our costing sets the max and min values using timestamp format)
            } else if(dataType.equals(DataTypeManager.DefaultDataClasses.DATE)) {
                compareValue = ((Date)value.getValue()).getTime();
                maxValue = Timestamp.valueOf(max).getTime();
                minValue = Timestamp.valueOf(min).getTime();
            } else {
            	if(!Number.class.isAssignableFrom(dataType)) {
                    if (unknownChildCost) {
                        return UNKNOWN_VALUE;
                    }
                    return childCost/3;
                }
                compareValue = ((Number)value.getValue()).floatValue();
                maxValue = Integer.parseInt(max);
                minValue = Integer.parseInt(min);
            }
            float range = Math.max(maxValue - minValue, 1);
            
            float costMultiple = 1;
            
            if(compCrit.getOperator() == CompareCriteria.GT || compCrit.getOperator() == CompareCriteria.GE) {
            	costMultiple = (maxValue - compareValue)/range;
                if (compareValue < 0 && maxValue < 0) {
                	costMultiple = (1 - costMultiple);
                }
            } else if(compCrit.getOperator() == CompareCriteria.LT || compCrit.getOperator() == CompareCriteria.LE) {
            	costMultiple = (compareValue - minValue)/range;
                if (compareValue < 0 && minValue < 0) {
                	costMultiple = (1 - costMultiple);
                }
            }
            if (costMultiple > 1) {
            	costMultiple = 1;
            } else if (costMultiple < 0) {
            	costMultiple = 0;
            }
            cost = childCost * costMultiple;
        }catch(IllegalArgumentException e) {
            LogManager.logWarning(LogConstants.CTX_QUERY_PLANNER, e, QueryPlugin.Util.getString("NewCalculateCostUtil.badCost")); //$NON-NLS-1$
            // If we were unable to parse the timestamp we will revert to the divide by three estimate
            if (unknownChildCost) {
                return UNKNOWN_VALUE;
            }
            cost = childCost/3;
        }
        return cost;
    }
    
    static boolean usesKey(PlanNode planNode, Collection<? extends SingleElementSymbol> allElements, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
    	//TODO: key preserved joins should be marked
    	return isSingleTable(planNode)
    	&& usesKey(allElements, metadata);
    }

	static boolean isSingleTable(PlanNode planNode) {
		return NodeEditor.findAllNodes(planNode, NodeConstants.Types.SOURCE, NodeConstants.Types.JOIN | NodeConstants.Types.SET_OP).size() == 1;
	}
    
    public static boolean usesKey(Criteria crit, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        HashSet<ElementSymbol> elements = new HashSet<ElementSymbol>();
        collectElementsOfValidCriteria(crit, elements);
        return usesKey(elements, metadata);
    }
    
    /**
     * TODO: this uses key check is not really accurate, it doesn't take into consideration where 
     * we are in the plan.
     * if a key column is used after a non 1-1 join or a union all, then it may be non-unique.
     */
    public static boolean usesKey(Collection<? extends SingleElementSymbol> allElements, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {
    	return usesKey(allElements, null, metadata, true);
    }

    public static boolean usesKey(Collection<? extends SingleElementSymbol> allElements, Set<GroupSymbol> groups, QueryMetadataInterface metadata, boolean unique)
    throws QueryMetadataException, TeiidComponentException {
		return getKeyUsed(allElements, groups, metadata, unique) != null;
    }
    
    public static Object getKeyUsed(Collection<? extends SingleElementSymbol> allElements, Set<GroupSymbol> groups, QueryMetadataInterface metadata, Boolean unique)
    throws QueryMetadataException, TeiidComponentException {
        
        if(allElements == null || allElements.size() == 0) { 
            return null;
        }    
     
        // Sort elements into groups
        Map<GroupSymbol, List<Object>> groupMap = new HashMap<GroupSymbol, List<Object>>();
        for (SingleElementSymbol ses : allElements) {
        	Expression ex = SymbolMap.getExpression(ses); 
        	if (!(ex instanceof ElementSymbol)) {
        		continue; //TODO: function based indexes are possible, but we don't have the metadata
        	}
        	ElementSymbol element = (ElementSymbol)ex;
            GroupSymbol group = element.getGroupSymbol();
            if (groups != null && !groups.contains(group)) {
            	continue;
            }
            List<Object> elements = groupMap.get(group);
            if(elements == null) { 
                elements = new ArrayList<Object>();
                groupMap.put(group, elements);
            }
            elements.add(element.getMetadataID());
        }    
             
        // Walk through each group
        for (Map.Entry<GroupSymbol, List<Object>> entry : groupMap.entrySet()) {
            GroupSymbol group = entry.getKey();
            List<Object> elements = entry.getValue();
            
            // Look up keys
            Collection keys = null;
            if ((unique != null && unique) || unique == null) {
            	keys = metadata.getUniqueKeysInGroup(group.getMetadataID());
            } 
            if ((unique != null && !unique) || unique == null) {
            	if (keys != null) {
            		keys = new ArrayList<Object>(keys);
            	} else {
            		keys = new ArrayList<Object>(2);
            	}
            	keys.addAll(metadata.getIndexesInGroup(group.getMetadataID()));
            }
            
            if(keys != null && keys.size() > 0) { 
                // For each key, get key elements
            	for (Object key : keys) {
                    List keyElements = metadata.getElementIDsInKey(key);
                    if(elements.containsAll(keyElements)) {
                        // Used all elements of the key
                        return key;
                    }    
                }
            }                                    
        }
        
        return null; 
    }    
    
    private static float safeLog(float x) {
        return (float)Math.max(1, Math.log(x));
    }
    
    /**
     * Computes the cost of a Dependent Join
     * 
     * The worst possible cost will arise from a high independent ndv (many dependent sets) and a low dependent ndv (possibly many matches per set)
     * 
     * This logic uses the same assumption as criteria in that ndv is used as a divisor of cardinality. 
     * @throws QueryPlannerException 
     * 
     */
    public static DependentCostAnalysis computeCostForDepJoin(PlanNode joinNode, boolean leftIndependent, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, CommandContext context) 
        throws TeiidComponentException, QueryMetadataException, QueryPlannerException {
        
        PlanNode independentNode = leftIndependent?joinNode.getFirstChild():joinNode.getLastChild();
        PlanNode dependentNode = leftIndependent?joinNode.getLastChild():joinNode.getFirstChild();
        
        List independentExpressions = (List)(leftIndependent?joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS):joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS));
        List dependentExpressions = (List)(leftIndependent?joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS):joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS));
        
        return computeCostForDepJoin(independentNode, dependentNode,
				independentExpressions, dependentExpressions, metadata,
				capFinder, context);
    }
    
	public static DependentCostAnalysis computeCostForDepJoin(PlanNode independentNode,
			PlanNode dependentNode, List independentExpressions,
			List dependentExpressions, QueryMetadataInterface metadata,
			CapabilitiesFinder capFinder, CommandContext context)
			throws QueryMetadataException, TeiidComponentException, QueryPlannerException {

        float independentCardinality = computeCostForTree(independentNode, metadata);
        float dependentCardinality = computeCostForTree(dependentNode, metadata);

        DependentCostAnalysis dca = new DependentCostAnalysis();
        dca.maxNdv = new Float[independentExpressions.size()];
        dca.expectedNdv = new Float[independentExpressions.size()];

        if (independentCardinality == UNKNOWN_VALUE || dependentCardinality == UNKNOWN_VALUE) {
        	return dca; //no cost information to be determined
        }
        
        float processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
        if(context != null) {
            processorBatchSize = context.getProcessorBatchSize();
        }

        RulePushSelectCriteria rpsc = new RulePushSelectCriteria();
        rpsc.setCreatedNodes(new LinkedList<PlanNode>());
        
		for (int i = 0; i < independentExpressions.size(); i++) {
			Expression indExpr = (Expression)independentExpressions.get(i);
			Collection<ElementSymbol> indElements = ElementCollectorVisitor.getElements(indExpr, true);
			float indSymbolNDV = getNDVEstimate(independentNode, metadata, independentCardinality, indElements, true);
			Expression depExpr = (Expression)dependentExpressions.get(i);
			
			LinkedList<Expression> depExpressions = new LinkedList<Expression>();
			LinkedList<PlanNode> targets = determineTargets(dependentNode,
					metadata, capFinder, rpsc, depExpr, depExpressions);
			
			Iterator<Expression> exprIter = depExpressions.iterator(); 
			for (Iterator<PlanNode> targetIter = targets.iterator(); targetIter.hasNext();) {
				PlanNode target = targetIter.next();
				Expression targerDepExpr = exprIter.next();
				PlanNode accessNode = NodeEditor.findParent(target, NodeConstants.Types.ACCESS);

		        float setCriteriaBatchSize = indSymbolNDV;
                
		        if (accessNode != null) {
		            setCriteriaBatchSize = CapabilitiesUtil.getMaxInCriteriaSize(RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata), metadata, capFinder);
		            if (setCriteriaBatchSize < 1) {
		                setCriteriaBatchSize = indSymbolNDV;
		            } else {
		            	int numberOfSets = CapabilitiesUtil.getMaxDependentPredicates(RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata), metadata, capFinder);
		            	if (numberOfSets > 0) {
		            		setCriteriaBatchSize *= Math.max(1, numberOfSets /dependentExpressions.size()); //scale down to be conservative 
		            	}
		            }
		        } else if (indSymbolNDV > processorBatchSize) {
	            	//don't bother making a virtual join dependent if they are likely to be large
		        	//TODO: what operations are performed between origNode and dependentNode
	        		//TODO: we should be using a tree structure rather than just a value iterator
	        		continue;
		        }
		        if (target.hasBooleanProperty(Info.MAKE_NOT_DEP)) {
		        	continue;
		        }
	        	Collection<ElementSymbol> depElems = ElementCollectorVisitor.getElements(targerDepExpr, true);
	        	while (target.getParent().getType() == NodeConstants.Types.SELECT) {
	        		target = target.getParent();
	        	}
	        	float depTargetCardinality = computeCostForTree(target, metadata);
	        	if (depTargetCardinality == UNKNOWN_VALUE) {
	        		continue;
	        	}
	        	float depSymbolNDV = getStat(Stat.NDV, depElems, target, depTargetCardinality, metadata);
	        	boolean usesKey = usesKey(dependentNode, depElems, metadata);
				if (depSymbolNDV == UNKNOWN_VALUE) {
					if (!usesKey) {
						//make an educated guess that this is a fk
						float indSymbolOrigNDV = indSymbolNDV;
						float indCardinalityOrig = independentCardinality;
						//TODO: we should probably dig deeper than this
						PlanNode indOrigNode = FrameUtil.findOriginatingNode(independentNode, GroupsUsedByElementsVisitor.getGroups(indElements));
						if (indOrigNode != null) {
							indCardinalityOrig = computeCostForTree(indOrigNode, metadata);
							indSymbolOrigNDV = getStat(Stat.NDV, indElements, indOrigNode, indCardinalityOrig, metadata);
							if (indSymbolOrigNDV == UNKNOWN_VALUE) {
								indSymbolOrigNDV = indCardinalityOrig * indSymbolNDV / independentCardinality;
							} 
						}
						depSymbolNDV = Math.max((float)Math.pow(depTargetCardinality, .75), Math.min(indSymbolOrigNDV, depTargetCardinality));
					} else {
						depSymbolNDV = depTargetCardinality;
					}
				}
				boolean usesIndex = accessNode != null && usesKey;
				if (!usesKey && accessNode != null && target.getType() == NodeConstants.Types.SOURCE && target.getChildCount() == 0) {
					usesIndex = usesKey(depElems, target.getGroups(), metadata, false);
				}
		        float[] estimates = estimateCost(accessNode, setCriteriaBatchSize, usesIndex, depTargetCardinality, indSymbolNDV, dependentCardinality, depSymbolNDV);
		        if (estimates[1] < 0) {
		        	if (dca.expectedCardinality == null) {
		        		dca.expectedCardinality = estimates[0];
		        	} else {
		        		dca.expectedCardinality = Math.min(dca.expectedCardinality, estimates[0]);
		        	}
		        }
		        dca.expectedNdv[i] = indSymbolNDV;
		        //use a quick binary search to find the max ndv
		        float min = 0;
		        float max = Math.max(Integer.MAX_VALUE, indSymbolNDV);
		        for (int j = 0; j < 10; j++) {
		        	if (estimates[1] > 1) {
		        		max = indSymbolNDV;
		        		indSymbolNDV = (indSymbolNDV + min)/2;
		        	} else if (estimates[1] < 0) {
		        		min = indSymbolNDV;
		        		//we assume that values should be closer to the min side
		        		indSymbolNDV = Math.min(indSymbolNDV * 8 + 1, (indSymbolNDV + max)/2);
		        	} else {
		        		break;
		        	}
		        	estimates = estimateCost(accessNode, setCriteriaBatchSize, usesIndex, depTargetCardinality, indSymbolNDV, dependentCardinality, depSymbolNDV);
		        }
		        dca.maxNdv[i] = indSymbolNDV;
			}
		}
        return dca;
	}
	
	private static float[] estimateCost(PlanNode accessNode, float setCriteriaBatchSize, boolean usesIndex, float depTargetCardinality, 
			float indSymbolNDV, float dependentCardinality, float depSymbolNDV) {
        float dependentAccessCardinality = Math.min(depTargetCardinality, depTargetCardinality * indSymbolNDV / depSymbolNDV);
        float scaledCardinality = Math.min(dependentCardinality, dependentCardinality * indSymbolNDV / depSymbolNDV);
		float numberComparisons = (usesIndex?safeLog(depTargetCardinality):depTargetCardinality) * (usesIndex?indSymbolNDV:safeLog(indSymbolNDV));
        float newDependentQueries = accessNode == null?0:(float)Math.ceil(indSymbolNDV / setCriteriaBatchSize);
        
        float relativeCost = newDependentQueries*procNewRequestTime;
        float relativeComparisonCost = (numberComparisons - safeLog(scaledCardinality) /*no longer needed by the join*/
            /*sort cost reduction, however it's always true if its on the source and using an index
              TODO: there are other cost reductions, which we could get by checking the other parent nodes */
        	+ (scaledCardinality*safeLog(scaledCardinality) - dependentCardinality*safeLog(dependentCardinality))) 
        	* compareTime;
        float relativeReadCost = (dependentAccessCardinality - depTargetCardinality)*readTime; //cardinality reductions
        return new float[] {scaledCardinality, relativeCost + relativeComparisonCost + relativeReadCost};
	}
	
	/**
	 * For now we only consider a single target. In the future we may consider multiple.
	 */
	private static LinkedList<PlanNode> determineTargets(
			PlanNode dependentNode, QueryMetadataInterface metadata,
			CapabilitiesFinder capFinder, RulePushSelectCriteria rpsc,
			Expression depExpr, LinkedList<Expression> depExpressions)
			throws QueryPlannerException, TeiidComponentException {
		LinkedList<PlanNode> targets = new LinkedList<PlanNode>();
		LinkedList<PlanNode> critNodes = new LinkedList<PlanNode>();
		critNodes.add(RelationalPlanner.createSelectNode(new DependentSetCriteria(depExpr, null), false));
		LinkedList<PlanNode> initialTargets = new LinkedList<PlanNode>();
		initialTargets.add(dependentNode);
		while (!critNodes.isEmpty()) {
			PlanNode critNode = critNodes.remove();
			PlanNode initial = initialTargets.remove();
			if (critNode.getGroups().isEmpty()) {
				//TODO: we need to project constants up through a plan to avoid this case
				continue;
			}
			PlanNode sourceNode = FrameUtil.findOriginatingNode(initial, critNode.getGroups());
			PlanNode target = sourceNode;
			if (initial != sourceNode) {
				target = rpsc.examinePath(initial, sourceNode, metadata, capFinder);					
			}
			if (target != sourceNode || (sourceNode.getType() == NodeConstants.Types.SOURCE && sourceNode.getChildCount() == 0)) {
				targets.add(target);
				DependentSetCriteria dsc = (DependentSetCriteria)critNode.getProperty(Info.SELECT_CRITERIA);
				depExpressions.add(dsc.getExpression());
				continue;
			}
			if (sourceNode.getType() == NodeConstants.Types.SOURCE) {
				PlanNode child = sourceNode.getFirstChild();
		        child = FrameUtil.findOriginatingNode(child, child.getGroups());
		        if (child != null && child.getType() == NodeConstants.Types.SET_OP) {
		        	targets.add(target);
					DependentSetCriteria dsc = (DependentSetCriteria)critNode.getProperty(Info.SELECT_CRITERIA);
					depExpressions.add(dsc.getExpression());
					//TODO: we need better handling for set op situations
					continue;
		        }
				if (!rpsc.pushAcrossFrame(sourceNode, critNode, metadata)) {
					targets.add(target);
					DependentSetCriteria dsc = (DependentSetCriteria)critNode.getProperty(Info.SELECT_CRITERIA);
					depExpressions.add(dsc.getExpression());
				}
				List<PlanNode> createdNodes = rpsc.getCreatedNodes();
				for (PlanNode planNode : createdNodes) {
					critNodes.add(planNode);
					initialTargets.add(planNode.getFirstChild());
					NodeEditor.removeChildNode(planNode.getParent(), planNode);
				}
				rpsc.getCreatedNodes().clear();
			} 
			//the source must be a null or project node, which we don't care about
		}
		return targets;
	}

	static float getNDVEstimate(PlanNode indNode,
			QueryMetadataInterface metadata, float cardinality,
			Collection<? extends SingleElementSymbol> elems, boolean useCardinalityIfUnknown) throws QueryMetadataException,
			TeiidComponentException {
		if (elems == null || elems.isEmpty()) {
			return cardinality;
		}
		float ndv = getStat(Stat.NDV, elems, indNode, cardinality, metadata);
		if (ndv == UNKNOWN_VALUE) { 
			if (cardinality == UNKNOWN_VALUE) {
				return UNKNOWN_VALUE;
			}
			if (usesKey(indNode, elems, metadata)) {
				ndv = cardinality;
			} else if (useCardinalityIfUnknown) {
				ndv = cardinality/2; 
			} else {
				return UNKNOWN_VALUE;
			}
		}
		return Math.max(1, ndv);
	}
    
}
