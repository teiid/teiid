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

import java.util.List;
import java.util.Map;

import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.processor.relational.SourceState.ImplicitBuffer;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.SymbolMap;

/**
 * Variation of a nested loop join that handles nested tables
 */
public class NestedTableJoinStrategy extends JoinStrategy {
	
	private SymbolMap leftMap;
	private SymbolMap rightMap;
	private Evaluator eval;
	private boolean outerMatched;

	@Override
	public NestedTableJoinStrategy clone() {
		NestedTableJoinStrategy clone = new NestedTableJoinStrategy();
		clone.leftMap = leftMap;
		clone.rightMap = rightMap;
		return clone;
	}
	
	@Override
	public void initialize(JoinNode joinNode) {
		super.initialize(joinNode);
		this.eval = new Evaluator(null, joinNode.getDataManager(), joinNode.getContext());
	}
	
	public void setLeftMap(SymbolMap leftMap) {
		this.leftMap = leftMap;
	}
	
	public void setRightMap(SymbolMap rightMap) {
		this.rightMap = rightMap;
	}
	
	@Override
	protected void openLeft() throws TeiidComponentException,
			TeiidProcessingException {
		if (leftMap == null) {
			super.openLeft();
		}
	}
	
	@Override
	protected void openRight() throws TeiidComponentException,
			TeiidProcessingException {
		if (rightMap == null) {
			super.openRight();
			this.rightSource.setImplicitBuffer(ImplicitBuffer.FULL);
		}
	}
	
	@Override
	protected void process() throws TeiidComponentException,
			TeiidProcessingException {
		
		if (leftMap != null && !leftSource.open) {
			for (Map.Entry<ElementSymbol, Expression> entry : leftMap.asMap().entrySet()) {
				joinNode.getContext().getVariableContext().setValue(entry.getKey(), eval.evaluate(entry.getValue(), null));
			}
			leftSource.getSource().reset();
			super.openLeft();
		}
		
		IndexedTupleSource its = leftSource.getIterator();
		
		while (its.hasNext() || leftSource.getCurrentTuple() != null) {
			
			List<?> leftTuple = leftSource.getCurrentTuple();
			if (leftTuple == null) {
				leftTuple = leftSource.saveNext();
			}
			updateContext(leftTuple, leftSource.getSource().getElements());
			
			if (rightMap != null && !rightSource.open) {
				for (Map.Entry<ElementSymbol, Expression> entry : rightMap.asMap().entrySet()) {
					joinNode.getContext().getVariableContext().setValue(entry.getKey(), eval.evaluate(entry.getValue(), null));
				}
				rightSource.getSource().reset();
				super.openRight();
			}
			
			IndexedTupleSource right = rightSource.getIterator();
			
			while (right.hasNext() || rightSource.getCurrentTuple() != null) {
				
				List<?> rightTuple = rightSource.getCurrentTuple();
				if (rightTuple == null) {
					rightTuple = rightSource.saveNext();
				}
				
				List<?> outputTuple = outputTuple(this.leftSource.getCurrentTuple(), this.rightSource.getCurrentTuple());
                
                boolean matches = this.joinNode.matchesCriteria(outputTuple);
                
                rightSource.saveNext();

                if (matches) {
                	outerMatched = true;
                	joinNode.addBatchRow(outputTuple);
                }
			}
			
			if (!outerMatched && this.joinNode.getJoinType() == JoinType.JOIN_LEFT_OUTER) {
            	joinNode.addBatchRow(outputTuple(this.leftSource.getCurrentTuple(), this.rightSource.getOuterVals()));
            }
			
			outerMatched = false;
			
			if (rightMap == null) {
				rightSource.getIterator().setPosition(1);
			} else {
				rightSource.close();
				for (Map.Entry<ElementSymbol, Expression> entry : rightMap.asMap().entrySet()) {
					joinNode.getContext().getVariableContext().remove(entry.getKey());
				}
			}
			
			leftSource.saveNext();
			updateContext(null, leftSource.getSource().getElements());
		}
		
		if (leftMap != null) {
			leftSource.close();
			for (Map.Entry<ElementSymbol, Expression> entry : leftMap.asMap().entrySet()) {
				joinNode.getContext().getVariableContext().remove(entry.getKey());
			}
		}
	}

	private void updateContext(List<?> tuple,
			List<? extends SingleElementSymbol> elements) {
		for (int i = 0; i < elements.size(); i++) {
			SingleElementSymbol element = elements.get(i);
			if (element instanceof ElementSymbol) {
				if (tuple == null) {
					joinNode.getContext().getVariableContext().remove((ElementSymbol)element);
				} else {
					joinNode.getContext().getVariableContext().setValue((ElementSymbol)element, tuple.get(i));
				}
			}
		}
	}
	
    public String toString() {
        return "NESTED TABLE JOIN"; //$NON-NLS-1$
    }

}
