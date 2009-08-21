package com.metamatrix.connector.salesforce.execution.visitors;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFromItem;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IJoin;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.Group;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.salesforce.Util;

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

public class JoinQueryVisitor extends SelectVisitor implements
		IQueryProvidingVisitor {

	private Group leftTableInJoin;
	private Group rightTableInJoin;
	private Group childTable;

	public JoinQueryVisitor(RuntimeMetadata metadata) {
		super(metadata);
	}

	// Has to be a left outer join
	@Override
	public void visit(IJoin join) {
		try {
			IFromItem left = join.getLeftItem();
			if (left instanceof IGroup) {
				IGroup leftGroup = (IGroup) left;
				leftTableInJoin = leftGroup.getMetadataObject();
				loadColumnMetadata(leftGroup);
			}

			IFromItem right = join.getRightItem();
			if (right instanceof IGroup) {
				IGroup rightGroup = (IGroup) right;
				rightTableInJoin = rightGroup.getMetadataObject();
				loadColumnMetadata((IGroup) right);
			}
			super.visit(join);
		} catch (ConnectorException ce) {
			exceptions.add(ce);
		}

	}

	@Override
	public void visit(ICompareCriteria criteria) {
		
		// Find the criteria that joins the two tables
		try {
			IExpression rExp = criteria.getRightExpression();
			if (rExp instanceof IElement) {
				IExpression lExp = criteria.getLeftExpression();
				if (isIdColumn((IExpression) rExp) || isIdColumn(lExp)) {

					Element rColumn = ((IElement) rExp).getMetadataObject();
					String rTableName = rColumn.getParent().getNameInSource();

					Element lColumn = ((IElement) lExp).getMetadataObject();
					String lTableName = lColumn.getParent().getNameInSource();

					if (leftTableInJoin.getNameInSource().equals(rTableName)
							|| leftTableInJoin.getNameInSource().equals(lTableName)
							&& rightTableInJoin.getNameInSource().equals(rTableName)
							|| rightTableInJoin.getNameInSource().equals(lTableName)
							&& !rTableName.equals(lTableName)) {
						// This is the join criteria, the one that is the ID is the parent.
						IExpression fKey = !isIdColumn(lExp) ? lExp : rExp; 
						table = childTable =  ((IElement) fKey).getMetadataObject().getParent();
					} else {
						// Only add the criteria to the query if it is not the join criteria.
						// The join criteria is implicit in the salesforce syntax.
						super.visit(criteria);
					}
				}
			} else {
				super.visit(criteria);
			}
		} catch (ConnectorException e) {
			exceptions.add(e);
		}
	}

	@Override
	public String getQuery() throws ConnectorException {
		
		if (isParentToChildJoin()) {
			return super.getQuery();
		} else {
			if (!exceptions.isEmpty()) {
				throw ((ConnectorException) exceptions.get(0));
			}
			StringBuffer select = new StringBuffer();
			select.append(SELECT).append(SPACE);
			addSelectSymbols(leftTableInJoin.getNameInSource(), select);
			select.append(COMMA).append(SPACE).append(OPEN);
			
			StringBuffer subselect = new StringBuffer();
			subselect.append(SELECT).append(SPACE);
			addSelectSymbols(rightTableInJoin.getNameInSource(), subselect);
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
	}

	public boolean isParentToChildJoin() {
		return childTable.equals(leftTableInJoin);
	}

	protected void addSelectSymbols(String tableNameInSource, StringBuffer result) throws ConnectorException {
		boolean firstTime = true;
		for (ISelectSymbol symbol : selectSymbols) {
			IExpression expression = symbol.getExpression();
			if (expression instanceof IElement) {
				Element element = ((IElement) expression).getMetadataObject();
				String tableName = element.getParent().getNameInSource();
				if(!isParentToChildJoin() && tableNameInSource.equals(tableName) ||
						isParentToChildJoin()) {
					if (!firstTime) {
						result.append(", ");
					} else {
						firstTime = false;
					}
					result.append(tableName);
					result.append('.');
					result.append(element.getNameInSource());
				}
			} else if (expression instanceof IAggregate) {
				if (!firstTime) {
					result.append(", ");
				} else {
					firstTime = false;
				}
				result.append("count()"); //$NON-NLS-1$
			}
		}
	}

}
