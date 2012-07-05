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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.TableFunctionReference;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;

public abstract class SubqueryAwareRelationalNode extends RelationalNode {

	private SubqueryAwareEvaluator evaluator;

	protected SubqueryAwareRelationalNode() {
		super();
	}
	
	public SubqueryAwareRelationalNode(int nodeID) {
		super(nodeID);
	}
	
	protected Evaluator getEvaluator(Map elementMap) {
		if (this.evaluator == null) {
			this.evaluator = new SubqueryAwareEvaluator(elementMap, getDataManager(), getContext(), getBufferManager());
		} else {
			this.evaluator.initialize(getContext(), getDataManager());
		}
		return this.evaluator;
	}
	
	@Override
	public void reset() {
		super.reset();
		if (evaluator != null) {
			evaluator.reset();
		}
	}
	
	@Override
	public void closeDirect() {
		if (evaluator != null) {
			evaluator.close();
		}
	}
	
	protected void setReferenceValues(TableFunctionReference ref) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
		if (ref.getCorrelatedReferences() == null) {
			return;
		}
		for (Map.Entry<ElementSymbol, Expression> entry : ref.getCorrelatedReferences().asMap().entrySet()) {
			getContext().getVariableContext().setValue(entry.getKey(), getEvaluator(Collections.emptyMap()).evaluate(entry.getValue(), null));
		}
	}
	
	abstract protected Collection<? extends LanguageObject> getObjects();
	
	@Override
	public Boolean requiresTransaction(boolean transactionalReads) {
		if (!transactionalReads) {
			return false;
		}
		return !ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(getObjects()).isEmpty();
	}

}
