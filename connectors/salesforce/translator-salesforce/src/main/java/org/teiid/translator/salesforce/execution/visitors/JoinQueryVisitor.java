package org.teiid.translator.salesforce.execution.visitors;

import org.teiid.core.util.StringUtil;
import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Join;
import org.teiid.language.Join.JoinType;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.TableReference;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.RuntimeMetadata;
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

    private NamedTable leftTableInJoin;
    private NamedTable rightTableInJoin;
    private NamedTable childTable;
    private String parentName;
    private ForeignKey foreignKey;
    private String childName;

    public JoinQueryVisitor(RuntimeMetadata metadata) {
        super(metadata);
    }

    // Has to be a join of only 2 tables.  criteria must be a compare
    @Override
    public void visit(Join join) {
        try {
            TableReference left = join.getLeftItem();
            NamedTable leftGroup = (NamedTable) left;
            leftTableInJoin = leftGroup;
            loadColumnMetadata(leftGroup);

            TableReference right = join.getRightItem();
            NamedTable rightGroup = (NamedTable) right;
            rightTableInJoin = rightGroup;
            loadColumnMetadata((NamedTable) right);
            Comparison criteria = (Comparison) join.getCondition();
            Expression lExp = criteria.getLeftExpression();
            Expression rExp = criteria.getRightExpression();
            if (isIdColumn(rExp) || isIdColumn(lExp)) {
                ColumnReference rColumn = (ColumnReference) rExp;
                NamedTable rTable = rColumn.getTable();

                ColumnReference lColumn = (ColumnReference) lExp;
                NamedTable lTable = lColumn.getTable();

                if (leftTableInJoin.equals(rTable)
                        || leftTableInJoin.equals(lTable)
                        && rightTableInJoin.equals(rTable)
                        || rightTableInJoin.equals(lTable)
                        && !lTable.equals(rTable)) {
                    // This is the join criteria, the one that is the ID is the parent.
                    Expression fKey = !isIdColumn(lExp) ? lExp : rExp;
                    ColumnReference columnReference = (ColumnReference) fKey;
                    table = childTable = columnReference.getTable();
                    String name = columnReference.getMetadataObject().getSourceName();
                    if (StringUtil.endsWithIgnoreCase(name, "id")) {
                        this.parentName = name.substring(0, name.length() - 2);
                    } else if (name.endsWith("__c")) { //$NON-NLS-1$
                        this.parentName = name.substring(0, name.length() - 1) + "r"; //$NON-NLS-1$
                    }
                    NamedTable parent = leftTableInJoin;
                    if (isChildToParentJoin()) {
                        parent = rightTableInJoin;
                    }
                    for (ForeignKey fk : childTable.getMetadataObject().getForeignKeys()) {
                        if (fk.getColumns().get(0).equals(columnReference.getMetadataObject()) && fk.getReferenceKey().equals(parent.getMetadataObject().getPrimaryKey())) {
                            foreignKey = fk;
                            break;
                        }
                    }
                    //inner joins require special handling as relationship queries are outer by default
                    if (join.getJoinType() == JoinType.INNER_JOIN) {
                        if (!isChildToParentJoin()) {
                            //flip the relationship
                            NamedTable t = leftTableInJoin;
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
        addSelect(leftTableInJoin, select, true);
        select.append(OPEN);

        StringBuilder subselect = new StringBuilder();
        subselect.append(SELECT).append(SPACE);
        addSelect(rightTableInJoin, subselect, false);
        subselect.append(SPACE);

        subselect.append(FROM).append(SPACE);

        if (this.foreignKey != null && this.foreignKey.getNameInSource() != null) {
            childName = this.foreignKey.getNameInSource();
        } else {
            childName = rightTableInJoin.getMetadataObject().getNameInSource() + "s"; //$NON-NLS-1$
        }
        subselect.append(childName);
        subselect.append(CLOSE).append(SPACE);

        select.append(subselect);

        select.append(FROM).append(SPACE);
        select.append(leftTableInJoin.getMetadataObject().getSourceName()).append(SPACE);
        addCriteriaString(select);
        appendGroupByHaving(select);
        select.append(limitClause);
        return select.toString();
    }

    @Override
    void appendColumnReference(StringBuilder queryString, ColumnReference ref) {
        if (isChildToParentJoin() && this.rightTableInJoin.equals(ref.getTable())
                && this.parentName != null) {
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

    void addSelect(NamedTable currentTable, StringBuilder result, boolean addComma) {
        boolean firstTime = true;
        for (DerivedColumn symbol : selectSymbols) {
            Expression expression = symbol.getExpression();
            if (expression instanceof ColumnReference) {
                ColumnReference element = (ColumnReference) expression;
                if(!isChildToParentJoin() && !currentTable.equals(element.getTable())) {
                    continue;
                }
                if (!firstTime) {
                    result.append(", "); //$NON-NLS-1$
                } else {
                    firstTime = false;
                }
                appendColumnReference(result, element);
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
        if (firstTime && !addComma) {
            result.append("id"); //$NON-NLS-1$
        } else if (!firstTime && addComma) {
            result.append(", "); //$NON-NLS-1$
        }
    }

    @Override
    public boolean canRetrieve() {
        return false;
    }

    public String getChildName() {
        return childName;
    }

    public NamedTable getRightTableInJoin() {
        return rightTableInJoin;
    }

    public NamedTable getLeftTableInJoin() {
        return leftTableInJoin;
    }

}
