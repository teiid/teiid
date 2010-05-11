package org.teiid.query.processor.relational;

import java.util.Map;

import org.teiid.query.eval.Evaluator;


public abstract class SubqueryAwareRelationalNode extends RelationalNode {

	private SubqueryAwareEvaluator evaluator;

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

}
