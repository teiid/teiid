/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.jpa;

import static org.teiid.language.SQLConstants.Reserved.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Function;
import org.teiid.language.Join;
import org.teiid.language.Join.JoinType;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
/**
 * This visitor converts the Teiid command into JPQL string
 */
public class JPQLSelectVisitor extends HierarchyVisitor {
    protected JPA2ExecutionFactory executionFactory;
    protected static final String UNDEFINED = "<undefined>"; //$NON-NLS-1$
    private LinkedList<JoinTable> joins = new LinkedList<JoinTable>();
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    protected LinkedHashMap<String, NamedTable> implicitGroups = new LinkedHashMap<String, NamedTable>();
    protected AtomicInteger aliasCounter = new AtomicInteger(0);
    protected RuntimeMetadata metadata;

    public JPQLSelectVisitor(JPA2ExecutionFactory executionFactory, RuntimeMetadata metadata) {
        super(false);
        this.executionFactory = executionFactory;
        this.metadata = metadata;
    }

    public static String getJPQLString(Select obj, JPA2ExecutionFactory executionFactory, RuntimeMetadata metadata)  throws TranslatorException {
        JPQLSelectVisitor visitor = new JPQLSelectVisitor(executionFactory, metadata);

        visitor.visitNode(obj);

        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }

        return visitor.convertToQuery(obj);
    }

    private String convertToQuery(Select obj) {
        JPQLSelectStringVisitor visitor = new JPQLSelectStringVisitor(this);
        visitor.visitNode(obj);
        return visitor.toString();
    }

    @Override
    public void visit(Select obj) {
        visitNodes(obj.getDerivedColumns());
        visitNodes(obj.getFrom());
        visitNode(obj.getWhere());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getOrderBy());
    }

    @Override
    public void visit(ColumnReference obj) {
        AbstractMetadataRecord record = obj.getMetadataObject();
        if (record != null) {
            String name = record.getProperty(JPAMetadataProcessor.KEY_ASSOSIATED_WITH_FOREIGN_TABLE, false);
            if (name != null) {
                try {
                    Table t = this.metadata.getTable(name);
                    String correlationName = obj.getTable().getCorrelationName();
                    String attrName = record.getProperty(JPAMetadataProcessor.RELATION_PROPERTY, false);
                    String groupName = correlationName + Tokens.DOT + attrName;
                    NamedTable nt = this.implicitGroups.get(groupName);
                    if (nt == null) {
                        nt = new NamedTable(groupName, "J_"+this.aliasCounter.getAndIncrement(), t);
                        this.implicitGroups.put(groupName, nt); //$NON-NLS-1$
                    }
                } catch (TranslatorException e) {
                    exceptions.add(e);
                }
            }
        }
    }

    private boolean alreadyInJoin(NamedTable table) {
        String[] splits = table.getName().split("\\.");
        String correlationName = splits[0];
        String attrName = splits[1];
        for(JoinTable joinTable:this.joins) {
            if (joinTable.parent != null
                    && joinTable.parent.getCorrelationName().equals(correlationName)
                    && attrName.equals(joinTable.childAttributeName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void visit(NamedTable obj) {
        if (implicitGroups.isEmpty()) {
            this.joins.add(new JoinTable(obj, null, JoinType.INNER_JOIN));
        }
        else {
            for (NamedTable table:this.implicitGroups.values()) {
                this.joins.add(new JoinTable(obj, table, JoinType.LEFT_OUTER_JOIN));
            }
        }
    }

    @Override
    public void visit(Join obj) {
        try {
            handleJoin(obj);
            for (NamedTable table:this.implicitGroups.values()) {
                NamedTable parent = findParent(table);
                if (parent != null) {
                    if (!alreadyInJoin(table)) {
                        this.joins.add(new JoinTable(parent, table, JoinType.LEFT_OUTER_JOIN));
                    }
                }
                else {
                    exceptions.add(new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14004, table.getName())));
                }
            }
        } catch (TranslatorException e) {
            exceptions.add(e);
        }
    }

    private NamedTable findParent(NamedTable child) {
        for (JoinTable jt:this.joins) {
            if (jt.getParent() != null) {
                if (isParentOf(jt.getParent(), child)) {
                    return jt.getParent();
                }
            }

            if (jt.getChild() != null) {
                if (isParentOf(jt.getChild(), child)) {
                    return jt.getChild();
                }
            }
        }
        return null;
    }

    private JoinTable handleJoin(Join obj)  throws TranslatorException {
        TableReference left = obj.getLeftItem();
        TableReference right = obj.getRightItem();

        if ((left instanceof NamedTable) && (right instanceof NamedTable)) {
            JoinTable join = handleJoin(obj.getJoinType(), left, right, obj.getCondition(), true);
            this.joins.add(join);
            return join;
        }

        JoinTable leftJoin = null;
        if (left instanceof Join) {
            leftJoin = handleJoin((Join)left);
            if (right instanceof NamedTable) {
                JoinTable join =  handleJoin(obj.getJoinType(), leftJoin, (NamedTable)right, obj.getCondition());
                this.joins.add(join);
                return join;
            }
        }

        JoinTable rightJoin = null;
        if (right instanceof Join) {
            rightJoin = handleJoin((Join)right);
            if (left instanceof NamedTable) {
                JoinTable join = handleJoin(obj.getJoinType(), (NamedTable)left, rightJoin, obj.getCondition());
                this.joins.add(join);
                return join;
            }
        }
        throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14005));
    }

    private JoinTable handleJoin(Join.JoinType joinType, JoinTable left, NamedTable right, Condition condition) throws TranslatorException {
        // first we need to find correct parent for the right
        JoinTable withParent = handleJoin(joinType, left.getParent(), right, condition, false);
        JoinTable withChild = handleJoin(joinType, left.getChild(), right, condition, false);

        NamedTable parent = null;
        if (withParent != null && withParent.getParent() != null) {
            parent = withParent.getParent();
        } else if (withChild != null && withChild.getParent() != null) {
            parent = withChild.getParent();
        }
        if (parent != null) {
            return handleJoin(joinType, parent, right, condition, true);
        }
        throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14006));
    }

    private JoinTable handleJoin(Join.JoinType joinType, NamedTable left, JoinTable right, Condition condition) throws TranslatorException {
        // first we need to find correct parent for the left
        JoinTable withParent = handleJoin(joinType, left, right.getParent(), condition, false);
        JoinTable withChild = handleJoin(joinType, left, right.getChild(), condition, false);

        NamedTable parent = null;
        if (withParent != null && withParent.getParent() != null) {
            parent = withParent.getParent();
        } else if (withChild != null && withChild.getParent() != null) {
            parent = withChild.getParent();
        }
        if (parent != null) {
            return handleJoin(joinType, left, parent, condition, true);
        }
        throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14006));
    }

    private JoinTable handleJoin(Join.JoinType joinType, TableReference left, TableReference right, Condition condition, boolean fixCorrelatedNames) {
        // both sides are named tables
        NamedTable leftTable = (NamedTable)left;
        NamedTable rightTable = (NamedTable)right;

        /*
         * This makes sure the two sides are actually supposed to be joined.
         */
        // TODO Handle AndOr conditions (composite keys/where clause conditions)
        Comparison comp = (Comparison)condition;
        if (!isInComparison(leftTable, comp) || !isInComparison(rightTable, comp)) {
            return null;
        }

        JoinTable joinTable = new JoinTable(leftTable, rightTable, joinType);

        if (fixCorrelatedNames) {
            String groupName;

            // fix left table's correleated name
            groupName = rightTable.getCorrelationName() + Tokens.DOT + leftTable.getName();
            NamedTable table = this.implicitGroups.get(groupName);
            if (table != null) {
                table.setCorrelationName(leftTable.getCorrelationName());
            }

            // fix right table's correleated name
            groupName = leftTable.getCorrelationName() + Tokens.DOT + rightTable.getName();
            table = this.implicitGroups.get(groupName);
            if (table != null) {
                table.setCorrelationName(rightTable.getCorrelationName());
            }
        }
        return joinTable;
    }

    private boolean isInComparison(NamedTable table, Comparison comparison) {
        return isInReference(table, (ColumnReference)comparison.getLeftExpression())
                || isInReference(table, (ColumnReference)comparison.getRightExpression());
    }

    private boolean isInReference(NamedTable table, ColumnReference reference) {
        return table.getCorrelationName().equals(reference.getTable().getCorrelationName())
                && table.getName().equals(reference.getTable().getName());
    }

    private boolean isParentOf(NamedTable parent, NamedTable child) {
        String[] splits = child.getName().split("\\.");
        String correlation = splits[0];
        String attrName = splits[1];
        if (!parent.getCorrelationName().equals(correlation)) {
            return false;
        }
        for (ForeignKey fk:parent.getMetadataObject().getForeignKeys()){
            if (fk.getReferenceKey().getParent().equals(child.getMetadataObject())) {
                return true;
            }
        }
        return false;
    }

    static class JoinTable {
        NamedTable parent;
        NamedTable child;
        NamedTable left;
        NamedTable right;
        String childAttributeName;
        String parentAttributeName;
        JoinType joinType;

        JoinTable(NamedTable left, NamedTable right, JoinType type) {
            this.left = left;
            this.right = right;
            this.joinType = type;
            if (right == null) {
                this.parent = left;
                this.parentAttributeName = left.getName();
            }
            else {
                /*
                 * An implicit join must match up to the right attribute. That
                 * is found in the join's name which is in the form of
                 * $correlationName.$attributeName.
                 *
                 * Implicit joins are always left to right.
                 */
                String[] splits = right.getName().split("\\.");
                boolean isImplicit = splits.length > 1;
                String attrName = null;
                if (isImplicit) {
                    attrName = splits[1];
                }
                for (ForeignKey fk:left.getMetadataObject().getForeignKeys()){
                    if (fk.getReferenceKey().getParent().equals(right.getMetadataObject())
                            && (!isImplicit || attrName.equals(fk.getSourceName()))) {
                        this.parent = left;
                        this.child = right;
                        this.childAttributeName = fk.getSourceName();
                        this.parentAttributeName = left.getName();
                    }
                }
                if (this.parent == null) {
                    for (ForeignKey fk:right.getMetadataObject().getForeignKeys()){
                        if (fk.getReferenceKey().getParent().equals(left.getMetadataObject())) {
                            this.parent = right;
                            this.child = left;
                            this.childAttributeName = fk.getSourceName();
                            this.parentAttributeName = right.getName();
                        }
                    }
                }
            }
        }

        NamedTable getParent() {
            return this.parent;
        }

        NamedTable getChild() {
            return this.child;
        }

        NamedTable getLeft() {
            return this.left;
        }

        NamedTable getRight() {
            return this.right;
        }

        String childAttributeName() {
            return this.childAttributeName;
        }

        String parentAttributeName() {
            return this.parentAttributeName;
        }

        public boolean isLeftParent() {
            return this.left == this.parent;
        }
    }


    static class JPQLSelectStringVisitor extends SQLStringVisitor {
        private JPQLSelectVisitor visitor;

        public JPQLSelectStringVisitor(JPQLSelectVisitor visitor) {
            this.visitor = visitor;
        }

        @Override
        public void visit(Select obj) {

            buffer.append(SELECT).append(Tokens.SPACE);

            if (obj.isDistinct()) {
                buffer.append(DISTINCT).append(Tokens.SPACE);
            }

            append(obj.getDerivedColumns());

            if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
                buffer.append(Tokens.SPACE).append(FROM).append(Tokens.SPACE);
                append(obj.getFrom());
            }

            if (obj.getWhere() != null) {
                buffer.append(Tokens.SPACE)
                      .append(WHERE)
                      .append(Tokens.SPACE);
                append(obj.getWhere());
            }
            if (obj.getGroupBy() != null) {
                buffer.append(Tokens.SPACE);
                append(obj.getGroupBy());
            }
            if (obj.getHaving() != null) {
                buffer.append(Tokens.SPACE)
                      .append(HAVING)
                      .append(Tokens.SPACE);
                append(obj.getHaving());
            }
            if (obj.getOrderBy() != null) {
                buffer.append(Tokens.SPACE);
                append(obj.getOrderBy());
            }
        }

        @Override
        public void visit(ColumnReference column) {
            Column record = column.getMetadataObject();
            if (record != null) {
                String name = record.getProperty(JPAMetadataProcessor.KEY_ASSOSIATED_WITH_FOREIGN_TABLE, false);
                if (name == null) {
                    buffer.append(column.getTable().getCorrelationName()).append(Tokens.DOT)
                            .append(record.getSourceName());
                }
                else {
                    String attrName = record.getProperty(JPAMetadataProcessor.RELATION_PROPERTY, false);
                    String attrColumnName = record.getProperty(JPAMetadataProcessor.RELATION_KEY, false);
                    String groupName = column.getTable().getCorrelationName() + Tokens.DOT + attrName;
                    String correlationName = this.visitor.implicitGroups.get(groupName).getCorrelationName();
                    buffer.append(correlationName).append(Tokens.DOT).append(attrColumnName);
                }
            }
            else {
                buffer.append(column.getName());
            }
        }

        @Override
        public void visit(Join obj) {
            addFromClause();
        }

        @Override
        public void visit(Function func) {
            if (visitor.executionFactory.getFunctionModifiers().containsKey(func.getName())) {
                visitor.executionFactory.getFunctionModifiers().get(func.getName()).translate(func);
            }
            super.visit(func);
        }

        @Override
        public void visit(NamedTable obj) {
            addFromClause();
        }

        private void addFromClause() {
            boolean first = true;
            for (JoinTable joinTable:this.visitor.joins) {
                if (!joinTable.isLeftParent() && joinTable.joinType == JoinType.LEFT_OUTER_JOIN) {
                    joinTable.joinType = JoinType.RIGHT_OUTER_JOIN;
                }
                if (first) {
                    buffer.append(joinTable.getParent().getName());
                    buffer.append(Tokens.SPACE);
                    buffer.append(AS).append(Tokens.SPACE);
                    buffer.append(joinTable.getParent().getCorrelationName());
                    first = false;
                }
                if (joinTable.getChild() != null) {
                    buffer.append(Tokens.SPACE);
                    switch(joinTable.joinType) {
                    case CROSS_JOIN:
                        buffer.append(CROSS);
                        break;
                    case FULL_OUTER_JOIN:
                        buffer.append(FULL)
                              .append(Tokens.SPACE)
                              .append(OUTER);
                        break;
                    case INNER_JOIN:
                        buffer.append(INNER);
                        break;
                    case LEFT_OUTER_JOIN:
                        buffer.append(LEFT)
                              .append(Tokens.SPACE)
                              .append(OUTER);
                        break;
                    case RIGHT_OUTER_JOIN:
                        buffer.append(RIGHT)
                              .append(Tokens.SPACE)
                              .append(OUTER);
                        break;
                    default: buffer.append(UNDEFINED);
                    }
                    buffer.append(Tokens.SPACE).append(JOIN).append(Tokens.SPACE);
                    buffer.append(joinTable.getParent().getCorrelationName()).append(Tokens.DOT).append(joinTable.childAttributeName());
                    buffer.append(Tokens.SPACE);
                    buffer.append(AS).append(Tokens.SPACE);
                    buffer.append(joinTable.getChild().getCorrelationName());
                }
            }
        }
    }
}
