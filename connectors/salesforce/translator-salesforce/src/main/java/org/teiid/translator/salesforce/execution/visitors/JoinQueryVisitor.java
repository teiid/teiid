package org.teiid.translator.salesforce.execution.visitors;

import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.Join.JoinType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
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
	private String parentName;
	private ForeignKey foreignKey;
	
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
				String rTableName = rColumn.getParent().getSourceName();

				Column lColumn = ((ColumnReference) lExp).getMetadataObject();
				String lTableName = lColumn.getParent().getSourceName();

				if (leftTableInJoin.getSourceName().equals(rTableName)
						|| leftTableInJoin.getSourceName().equals(lTableName)
						&& rightTableInJoin.getSourceName().equals(rTableName)
						|| rightTableInJoin.getSourceName().equals(lTableName)
						&& !rTableName.equals(lTableName)) {
					// This is the join criteria, the one that is the ID is the parent.
					Expression fKey = !isIdColumn(lExp) ? lExp : rExp;
					ColumnReference columnReference = (ColumnReference) fKey;
					table = childTable =  (Table)columnReference.getMetadataObject().getParent();
					String name = columnReference.getMetadataObject().getSourceName();
					if (StringUtil.endsWithIgnoreCase(name, "id")) {
						this.parentName = name.substring(0, name.length() - 2);
					} else if (name.endsWith("__c")) { //$NON-NLS-1$
					    this.parentName = name.substring(0, name.length() - 1) + "r"; //$NON-NLS-1$
					}
					Table parent = leftTableInJoin;
					if (isChildToParentJoin()) {
						parent = rightTableInJoin;
					}
					for (ForeignKey fk : childTable.getForeignKeys()) {
						if (fk.getColumns().get(0).equals(columnReference.getMetadataObject()) && fk.getReferenceKey().equals(parent.getPrimaryKey())) {
							foreignKey = fk;
							break;
						}
					}
					//inner joins require special handling as relationship queries are outer by default
					if (join.getJoinType() == JoinType.INNER_JOIN) {
						if (!isChildToParentJoin()) {
							//flip the relationship
							Table t = leftTableInJoin;
							this.leftTableInJoin = rightTableInJoin;
							this.rightTableInJoin = t;
						} 
   						//add is null criteria
						addCriteria(new Comparison(fKey, new Literal(null, fKey.getType()), Comparison.Operator.NE));
					}
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
		
		if (isChildToParentJoin()) {
			return super.getQuery();
		} 
		if (!exceptions.isEmpty()) {
			throw exceptions.get(0);
		}
		StringBuilder select = new StringBuilder();
		select.append(SELECT).append(SPACE);
		addSelect(leftTableInJoin.getSourceName(), select, true);
		select.append(OPEN);
		
		StringBuilder subselect = new StringBuilder();
		subselect.append(SELECT).append(SPACE);
		addSelect(rightTableInJoin.getSourceName(), subselect, false);
		subselect.append(SPACE);

		subselect.append(FROM).append(SPACE);
		
		String pluralName = null;
		if (this.foreignKey != null && this.foreignKey.getNameInSource() != null) {
			pluralName = this.foreignKey.getNameInSource();
		} else {
			pluralName = rightTableInJoin.getNameInSource() + "s"; //$NON-NLS-1$
		}
		subselect.append(pluralName);
    	subselect.append(CLOSE).append(SPACE);
    	
    	select.append(subselect);
		
    	select.append(FROM).append(SPACE);
		select.append(leftTableInJoin.getSourceName()).append(SPACE);
		addCriteriaString(select);
		appendGroupByHaving(select);
		select.append(limitClause);
		return select.toString();			
	}
	
	@Override
	void appendColumnReference(StringBuilder queryString, ColumnReference ref) {
		if (isChildToParentJoin() && this.rightTableInJoin.equals(ref.getMetadataObject().getParent()) 
				&& this.parentName != null) {
			//TODO: a self join won't work with this logic
			queryString.append(parentName);
			queryString.append('.');
			queryString.append(ref.getMetadataObject().getSourceName());
		} else {
			super.appendColumnReference(queryString, ref);
		}
	}

	public boolean isChildToParentJoin() {
		return childTable.equals(leftTableInJoin);
	}

	void addSelect(String tableNameInSource, StringBuilder result, boolean addComma) {
		boolean firstTime = true;
		for (DerivedColumn symbol : selectSymbols) {
			Expression expression = symbol.getExpression();
			if (expression instanceof ColumnReference) {
				Column element = ((ColumnReference) expression).getMetadataObject();
				String tableName = element.getParent().getSourceName();
				if(!isChildToParentJoin() && !tableNameInSource.equals(tableName)) {
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
				throw new AssertionError("Unknown select symbol type " + symbol); //$NON-NLS-1$
			}
		}
		if (!firstTime && addComma) {
			result.append(", "); //$NON-NLS-1$
		}
	}
	
	@Override
	public boolean canRetrieve() {
		return false;
	}

}
