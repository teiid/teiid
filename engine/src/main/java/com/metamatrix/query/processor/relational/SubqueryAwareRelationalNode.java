package com.metamatrix.query.processor.relational;

import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.query.eval.Evaluator;

public abstract class SubqueryAwareRelationalNode extends RelationalNode {

	private SubqueryAwareEvaluator evaluator;

	public SubqueryAwareRelationalNode(int nodeID) {
		super(nodeID);
	}
	
	protected Evaluator getEvaluator(Map elementMap) {
		if (this.evaluator == null) {
			this.evaluator = new SubqueryAwareEvaluator(elementMap, getDataManager(), getContext(), getBufferManager());
		} else {
			this.evaluator.setContext(getContext());
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
	public void close() throws MetaMatrixComponentException {
		super.close();
		if (evaluator != null) {
			evaluator.close();
		}
	}

}
