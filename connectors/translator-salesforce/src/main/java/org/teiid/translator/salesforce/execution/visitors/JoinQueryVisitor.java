package org.teiid.translator.salesforce.execution.visitors;

import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Join;
import org.teiid.language.NamedTable;
import org.teiid.language.TableReference;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;


/**
 * Salesforce supports joins only on primary key/foreign key relationships.  The connector
 * is supporting these joins through the OUTER JOIN syntax.  All RIGHT OUTER JOINS are 
 * rewritten by the query processor as LEFT OUTER JOINS, so that is all this visitor has
 * to expect.
 * 
 * Salesforce also requires a different syntax depending upon if you are joining from parent
 * to child, or from child to parent.
 * http://www.salesforce.com/us/developer/docs/api/index_Left.htm#StartTopic=Content/sforce_api_calls_soql_relationships.htm
 * 
 */

public class JoinQueryVisitor extends SelectVisitor {

	private Table leftTableInJoin;
	private Table rightTableInJoin;
	private Table childTable;

	public JoinQueryVisitor(RuntimeMetadata metadata) {
		super(metadata);
	}

	// Has to be a left outer join of only 2 tables.  criteria must be a compare
	@Override
	public void visit(Join join) {
		try {
			TableReference left = join.getLeftItem();
			NamedTable leftGroup = (NamedTable) left;
			leftTableInJoin = leftGroup.getMetadataObject();
			loadColumnMetadata(leftGroup);

			TableReference right = join.getRightItem();
			NamedTable rightGroup = (NamedTable) right;
			rightTableInJoin = rightGroup.getMetadataObject();
			loadColumnMetadata((NamedTable) right);
			Comparison criteria = (Comparison) join.getCondition();
			Expression lExp = criteria.getLeftExpression();
			Expression rExp = criteria.getRightExpression();
			if (isIdColumn(rExp) || isIdColumn(lExp)) {
				Column rColumn = ((ColumnReference) rExp).getMetadataObject();
				String rTableName = rColumn.getParent().getNameInSource();

				Column lColumn = ((ColumnReference) lExp).getMetadataObject();
				String lTableName = lColumn.getParent().getNameInSource();

				if (leftTableInJoin.getNameInSource().equals(rTableName)
						|| leftTableInJoin.getNameInSource().equals(lTableName)
						&& rightTableInJoin.getNameInSource().equals(rTableName)
						|| rightTableInJoin.getNameInSource().equals(lTableName)
						&& !rTableName.equals(lTableName)) {
					// This is the join criteria, the one that is the ID is the parent.
					Expression fKey = !isIdColumn(lExp) ? lExp : rExp; 
					table = childTable =  (Table)((ColumnReference) fKey).getMetadataObject().getParent();
				} else {
					// Only add the criteria to the query if it is not the join criteria.
					// The join criteria is implicit in the salesforce syntax.
					super.visit(criteria); //TODO: not valid
				}
			} else {
				super.visit(criteria); //TODO: not valid
			}
		} catch (TranslatorException ce) {
			exceptions.add(ce);
		}
	}

	@Override
	public String getQuery() throws TranslatorException {
		
		if (isParentToChildJoin()) {
			return super.getQuery();
		} 
		if (!exceptions.isEmpty()) {
			throw exceptions.get(0);
		}
		StringBuilder select = new StringBuilder();
		select.append(SELECT).append(SPACE);
		addSelect(leftTableInJoin.getNameInSource(), select, true);
		select.append(OPEN);
		
		StringBuilder subselect = new StringBuilder();
		subselect.append(SELECT).append(SPACE);
		addSelect(rightTableInJoin.getNameInSource(), subselect, false);
		subselect.append(SPACE);
		subselect.append(FROM).append(SPACE);
		subselect.append(rightTableInJoin.getNameInSource()).append('s');
		subselect.append(CLOSE).append(SPACE);
		select.append(subselect);
		select.append(FROM).append(SPACE);
		select.append(leftTableInJoin.getNameInSource()).append(SPACE);
		addCriteriaString(select);
		appendGroupByHaving(select);
		select.append(limitClause);
		return select.toString();			
	}

	public boolean isParentToChildJoin() {
		return childTable.equals(leftTableInJoin);
	}

	void addSelect(String tableNameInSource, StringBuilder result, boolean addComma) {
		boolean firstTime = true;
		for (DerivedColumn symbol : selectSymbols) {
			Expression expression = symbol.getExpression();
			if (expression instanceof ColumnReference) {
				Column element = ((ColumnReference) expression).getMetadataObject();
				String tableName = element.getParent().getNameInSource();
				if(!isParentToChildJoin() && !tableNameInSource.equals(tableName)) {
					continue;
				}
				if (!firstTime) {
					result.append(", "); //$NON-NLS-1$
				} else {
					firstTime = false;
				}
				appendColumnReference(result, (ColumnReference) expression);
			} else if (expression instanceof AggregateFunction) {
				if (!firstTime) {
					result.append(", "); //$NON-NLS-1$
				} else {
					firstTime = false;
				}
				appendAggregateFunction(result, (AggregateFunction)expression);
			} else {
				throw new AssertionError("Unknown select symbol type" + symbol); //$NON-NLS-1$
			}
		}
		if (!firstTime && addComma) {
			result.append(", "); //$NON-NLS-1$
		}
	}

}
