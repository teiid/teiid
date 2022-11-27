package org.teiid.translator.salesforce.execution.visitors;

import java.util.HashMap;
import java.util.Map;

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

    private NamedTable rootTable;

    private Map<NamedTable, String> parents = new HashMap<NamedTable, String>();

    /*
     * single child if applicable
     */
    private ForeignKey foreignKey;
    private NamedTable childTable;
    private String childName;

    public JoinQueryVisitor(RuntimeMetadata metadata) {
        super(metadata);
    }

    /*
     * We can treat left outer join as associative given the expectations of the on
     * clause
     */
    @Override
    public void visit(Join join) {
        try {
            TableReference left = join.getLeftItem();
            if (!(left instanceof NamedTable)) {
                visit((Join)left);
            } else {
                NamedTable leftGroup = (NamedTable) left;
                rootTable = leftGroup;
                loadColumnMetadata(leftGroup);
            }

            TableReference right = join.getRightItem();
            if (!(right instanceof NamedTable)) {
                throw new AssertionError("nested right join not expected"); //$NON-NLS-1$
            }

            NamedTable rightGroup = (NamedTable) right;
            loadColumnMetadata(rightGroup);
            Comparison criteria = (Comparison) join.getCondition();
            ColumnReference lExp = (ColumnReference) criteria.getLeftExpression();
            ColumnReference rExp = (ColumnReference) criteria.getRightExpression();
            ColumnReference fKey = !isIdColumn(lExp) ? lExp : rExp;
            ColumnReference pKey = isIdColumn(lExp) ? lExp : rExp;
            if (childTable == null) {
                //assume that the first right table determines the child
                childTable = fKey.getTable();
                NamedTable parent = isChildToParentJoin()?rightGroup:rootTable;
                for (ForeignKey fk : childTable.getMetadataObject().getForeignKeys()) {
                    if (fk.getColumns().get(0).equals(fKey.getMetadataObject()) && fk.getReferenceKey().equals(parent.getMetadataObject().getPrimaryKey())) {
                        foreignKey = fk;
                        break;
                    }
                }
                if (join.getJoinType() == JoinType.INNER_JOIN && !isChildToParentJoin()) {
                    //flip the relationship
                    this.rootTable = rightGroup;
                }
            }

            //determine the appropriate parent path
            if (isChildToParentJoin() || !rootTable.equals(pKey.getTable())) {
                String parentName = null;
                String name = fKey.getMetadataObject().getSourceName();
                if (StringUtil.endsWithIgnoreCase(name, "id")) {
                    parentName = name.substring(0, name.length() - 2);
                } else if (name.endsWith("__c")) { //$NON-NLS-1$
                    parentName = name.substring(0, name.length() - 1) + "r"; //$NON-NLS-1$
                }
                String baseName = parents.get(fKey.getTable());
                if (baseName != null) {
                    parentName = baseName + "." + parentName; //$NON-NLS-1$
                } else if (!rootTable.equals(fKey.getTable())) {
                    //the parent is not in the parentage
                    throw new AssertionError("cannot make a child reference after the first join"); //$NON-NLS-1$
                }
                this.parents.put(pKey.getTable(), parentName);
            }

            //inner joins require special handling as relationship queries are outer by default
            if (join.getJoinType() == JoinType.INNER_JOIN) {
                //add is null criteria
                addCriteria(new Comparison(fKey, new Literal(null, fKey.getType()), Comparison.Operator.NE));
            }
        } catch (TranslatorException ce) {
            exceptions.add(ce);
        }
        table = rootTable;
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
        addSelect(false, select, true);
        select.append(OPEN);

        StringBuilder subselect = new StringBuilder();
        subselect.append(SELECT).append(SPACE);
        addSelect(true, subselect, false);
        subselect.append(SPACE);

        subselect.append(FROM).append(SPACE);

        if (this.foreignKey != null && this.foreignKey.getNameInSource() != null) {
            childName = this.foreignKey.getNameInSource();
        } else {
            childName = childTable.getMetadataObject().getNameInSource() + "s"; //$NON-NLS-1$
        }
        subselect.append(childName);
        subselect.append(CLOSE).append(SPACE);

        select.append(subselect);

        select.append(FROM).append(SPACE);
        select.append(rootTable.getMetadataObject().getSourceName()).append(SPACE);
        addCriteriaString(select);
        appendGroupByHaving(select);
        select.append(limitClause);
        return select.toString();
    }

    @Override
    void appendColumnReference(StringBuilder queryString, ColumnReference ref) {
        String parentPath = parents.get(ref.getTable());
        if (parentPath != null && !rootTable.equals(ref.getTable())) {
            queryString.append(parentPath);
            queryString.append('.');
        }
        super.appendColumnReference(queryString, ref);
    }

    public boolean isChildToParentJoin() {
        return childTable.equals(rootTable);
    }

    void addSelect(boolean child, StringBuilder result, boolean addComma) {
        boolean firstTime = true;
        for (DerivedColumn symbol : selectSymbols) {
            Expression expression = symbol.getExpression();
            if (expression instanceof ColumnReference) {
                ColumnReference element = (ColumnReference) expression;
                if((!child && element.getTable().equals(childTable))
                || (child && (!element.getTable().equals(childTable)))) {
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

    /**
     * Get the child name - non-null only on a parent to child join
     * @return
     */
    public String getChildName() {
        return childName;
    }

    public NamedTable getChildTable() {
        return childTable;
    }

    public NamedTable getRootTable() {
        return rootTable;
    }

    public Map<NamedTable, String> getParents() {
        return parents;
    }

}
