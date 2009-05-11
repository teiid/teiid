package org.teiid.dqp.internal.datamgr.language;

import java.util.List;

import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IInsertExpressionValueSource;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;

public class InsertValueExpressionsImpl extends BaseLanguageObject implements IInsertExpressionValueSource {

	private List<IExpression> values;
	
	public InsertValueExpressionsImpl(List<IExpression> values) {
		this.values = values;
	}

	@Override
	public List<IExpression> getValues() {
		return values;
	}

	@Override
	public void acceptVisitor(LanguageObjectVisitor visitor) {
		visitor.visit(this);
	}
	
}
