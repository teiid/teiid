package org.teiid.query.sql.lang;

import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class WithQueryCommand implements LanguageObject, SubqueryContainer<QueryCommand> {
	
	private GroupSymbol groupSymbol;
	private List<ElementSymbol> columns;
	private QueryCommand queryExpression;
	
	public WithQueryCommand(GroupSymbol groupSymbol, List<ElementSymbol> columns, QueryCommand queryExpression) {
		this.groupSymbol = groupSymbol;
		this.columns = columns;
		this.queryExpression = queryExpression;
	}
	
	public GroupSymbol getGroupSymbol() {
		return groupSymbol;
	}

	public void setColumns(List<ElementSymbol> columns) {
		this.columns = columns;
	}
	
	public List<ElementSymbol> getColumns() {
		return columns;
	}
	
	@Override
	public QueryCommand getCommand() {
		return queryExpression;
	}
	
	public void setCommand(QueryCommand command) {
		this.queryExpression = command;
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public WithQueryCommand clone() {
		return new WithQueryCommand(groupSymbol.clone(), LanguageObject.Util.deepClone(columns, ElementSymbol.class), (QueryCommand)queryExpression.clone());
	}
	
	@Override
	public int hashCode() {
		return groupSymbol.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof WithQueryCommand)) {
			return false;
		}
		WithQueryCommand other = (WithQueryCommand)obj;
		return EquivalenceUtil.areEqual(groupSymbol, other.getGroupSymbol()) &&
		EquivalenceUtil.areEqual(this.columns, other.getColumns()) &&
		EquivalenceUtil.areEqual(this.queryExpression, other.queryExpression);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
}
