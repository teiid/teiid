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
import org.teiid.translator.salesforce.Util;


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

	// Has to be a left outer join
	@Override
	public void visit(Join join) {
		try {
			TableReference left = join.getLeftItem();
			if (left instanceof NamedTable) {
				NamedTable leftGroup = (NamedTable) left;
				leftTableInJoin = leftGroup.getMetadataObject();
				loadColumnMetadata(leftGroup);
			}

			TableReference right = join.getRightItem();
			if (right instanceof NamedTable) {
				NamedTable rightGroup = (NamedTable) right;
				rightTableInJoin = rightGroup.getMetadataObject();
				loadColumnMetadata((NamedTable) right);
			}
			super.visit(join);
		} catch (TranslatorException ce) {
			exceptions.add(ce);
		}

	}

	@Override
	public void visit(Comparison criteria) {
		// Find the criteria that joins the two tables
		Expression rExp = criteria.getRightExpression();
		if (rExp instanceof ColumnReference) {
			Expression lExp = criteria.getLeftExpression();
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
					super.visit(criteria);
				}
			}
		} else {
			super.visit(criteria);
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
		StringBuffer select = new StringBuffer();
		select.append(SELECT).append(SPACE);
		addSelect(leftTableInJoin.getNameInSource(), select, true);
		select.append(OPEN);
		
		StringBuffer subselect = new StringBuffer();
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
		select.append(limitClause);
		Util.validateQueryLength(select);
		return select.toString();			
	}

	public boolean isParentToChildJoin() {
		return childTable.equals(leftTableInJoin);
	}

	void addSelect(String tableNameInSource, StringBuffer result, boolean addComma) {
		boolean firstTime = true;
		for (DerivedColumn symbol : selectSymbols) {
			Expression expression = symbol.getExpression();
			if (expression instanceof ColumnReference) {
				Column element = ((ColumnReference) expression).getMetadataObject();
				String tableName = element.getParent().getNameInSource();
				if(!isParentToChildJoin() && tableNameInSource.equals(tableName) ||
						isParentToChildJoin()) {
					if (!firstTime) {
						result.append(", "); //$NON-NLS-1$						
					} else {
						firstTime = false;
					}
					result.append(tableName);
					result.append('.');
					result.append(element.getNameInSource());
				} else {
					continue;
				}
			} else if (expression instanceof AggregateFunction) {
				if (!firstTime) {
					result.append(", "); //$NON-NLS-1$
				} else {
					firstTime = false;
				}
				result.append("count()"); //$NON-NLS-1$
			} else {
				throw new AssertionError("Unknown select symbol type" + symbol); //$NON-NLS-1$
			}
		}
		if (!firstTime && addComma) {
			result.append(", "); //$NON-NLS-1$
		}
	}

}
