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
package org.teiid.translator.odata;

import static org.teiid.language.SQLConstants.Reserved.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.core.UriBuilder;

import org.teiid.language.*;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TranslatorException;

public class ODataSQLVisitor extends HierarchyVisitor {
    private static Map<String, String> infixFunctions = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    static {
        infixFunctions.put("%", "mod");//$NON-NLS-1$ //$NON-NLS-2$
        infixFunctions.put("mod", "mod");//$NON-NLS-1$ //$NON-NLS-2$
        infixFunctions.put("+", "add");//$NON-NLS-1$ //$NON-NLS-2$
        infixFunctions.put("-", "sub");//$NON-NLS-1$ //$NON-NLS-2$
        infixFunctions.put("*", "mul");//$NON-NLS-1$ //$NON-NLS-2$
        infixFunctions.put("/", "div");//$NON-NLS-1$ //$NON-NLS-2$
    }

    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    protected QueryExpression command;
    protected ODataExecutionFactory executionFactory;
    protected RuntimeMetadata metadata;
    protected ArrayList<Column> selectColumns = new ArrayList<Column>();
    protected StringBuilder filter = new StringBuilder();
    private EntitiesInQuery entities = new EntitiesInQuery();
    private Integer skip;
    private Integer top;
    private StringBuilder orderBy = new StringBuilder();
    private boolean count = false;

    public Column[] getSelect(){
        return this.selectColumns.toArray(new Column[this.selectColumns.size()]);
    }

    public boolean isCount() {
        return this.count;
    }

    public boolean isKeyLookup() {
        return this.entities.isKeyLookup();
    }

    public Table getEnityTable() {
        return this.entities.getFinalEntity();
    }

    public String getEnitityURL() {
        StringBuilder url = new StringBuilder();
        this.entities.append(url);
        return url.toString();
    }

    public String buildURL() {
        StringBuilder url = new StringBuilder();
        this.entities.append(url);
        if (this.count) {
            url.append("/$count"); //$NON-NLS-1$
        }
        UriBuilder uriBuilder = UriBuilder.fromPath(url.toString());

        if (this.filter.length() > 0) {
            uriBuilder.queryParam("$filter", this.filter.toString()); //$NON-NLS-1$
        }

        if (this.orderBy.length() > 0) {
            uriBuilder.queryParam("$orderby", this.orderBy.toString()); //$NON-NLS-1$
        }

        if (!this.selectColumns.isEmpty()) {
            LinkedHashSet<String> select = new LinkedHashSet<String>();
            for (Column column:this.selectColumns) {
                select.add(getColumnName(column));
            }
            StringBuilder sb = new StringBuilder();
            Iterator<String> it = select.iterator();
            while(it.hasNext()) {
                sb.append(it.next());
                if (it.hasNext()) {
                    sb.append(Tokens.COMMA);
                }
            }
            uriBuilder.queryParam("$select", sb.toString()); //$NON-NLS-1$
        }
        if (this.skip != null) {
            uriBuilder.queryParam("$skip", this.skip); //$NON-NLS-1$
        }
        if (this.top != null) {
            uriBuilder.queryParam("$top", this.top); //$NON-NLS-1$
        }
        //if (!this.count) {
        //    uriBuilder.queryParam("$format", "atom"); //$NON-NLS-1$ //$NON-NLS-2$
        //}

        URI uri = uriBuilder.build();
        return uri.toString();
    }

    public ODataSQLVisitor(ODataExecutionFactory executionFactory,
            RuntimeMetadata metadata) {
        this.executionFactory = executionFactory;
        this.metadata = metadata;
    }

    @Override
    public void visit(Comparison obj) {
        Expression left = obj.getLeftExpression();

        if(!this.executionFactory.supportsOdataBooleanFunctionsWithComparison()
                && left instanceof Function
                && "boolean".equals(((Function)left).getMetadataObject().getOutputParameter().getRuntimeType())) {
            visitComparisonWithBooleanFunction(obj);
            // early exit
            return;
        }
        append(left);
        this.filter.append(Tokens.SPACE);
        switch(obj.getOperator()) {
        case EQ:
            this.filter.append("eq"); //$NON-NLS-1$
            break;
        case NE:
            this.filter.append("ne"); //$NON-NLS-1$
            break;
        case LT:
            this.filter.append("lt"); //$NON-NLS-1$
            break;
        case LE:
            this.filter.append("le"); //$NON-NLS-1$
            break;
        case GT:
            this.filter.append("gt"); //$NON-NLS-1$
            break;
        case GE:
            this.filter.append("ge"); //$NON-NLS-1$
            break;
        }
        this.filter.append(Tokens.SPACE);
        appendRightComparison(obj);
    }

    public void visitComparisonWithBooleanFunction(Comparison obj) {
        boolean truthiness = SQLConstants.Reserved.TRUE.equals(obj.getRightExpression().toString());
        boolean isNot = !truthiness;
        switch(obj.getOperator()) {
        case EQ:
            break;
        case NE:
            isNot = !isNot;
            break;
        default:
            this.exceptions.add(new TranslatorException(
                    ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17018, ((Function)obj.getLeftExpression()).getName())));
        }
        if(isNot) {
            // can't use a Not object, because it requires a Condition inside,
            // and we don't have support for generic unary conditions
            this.filter.append(NOT)
                .append(Tokens.SPACE)
                .append(Tokens.LPAREN);
            append(obj.getLeftExpression());
            this.filter.append(Tokens.RPAREN);
        } else {
            append(obj.getLeftExpression());
        }
    }

    protected void appendRightComparison(Comparison obj) {
        append(obj.getRightExpression());
    }

    @Override
    public void visit(AndOr obj) {
        String opString = obj.getOperator().name().toLowerCase();
        appendNestedCondition(obj, obj.getLeftCondition());
        this.filter.append(Tokens.SPACE)
              .append(opString)
              .append(Tokens.SPACE);
        appendNestedCondition(obj, obj.getRightCondition());
    }

    protected void appendNestedCondition(AndOr parent, Condition condition) {
        if (condition instanceof AndOr) {
            AndOr nested = (AndOr)condition;
            if (nested.getOperator() != parent.getOperator()) {
                this.filter.append(Tokens.LPAREN);
                append(condition);
                this.filter.append(Tokens.RPAREN);
                return;
            }
        }
        append(condition);
    }

    @Override
    public void visit(ColumnReference obj) {
        this.filter.append(obj.getMetadataObject().getName());
    }

    protected boolean isInfixFunction(String function) {
        return infixFunctions.containsKey(function);
    }

    @Override
    public void visit(Function obj) {
        if (this.executionFactory.getFunctionModifiers().containsKey(obj.getName())) {
            List<?> parts = this.executionFactory.getFunctionModifiers().get(obj.getName()).translate(obj);
            if (parts != null) {
                throw new AssertionError("not supported"); //$NON-NLS-1$
            }
        }

        String name = obj.getName();
        List<Expression> args = obj.getParameters();
        if(isInfixFunction(name)) {
            this.filter.append(Tokens.LPAREN);
            if(args != null) {
                for(int i=0; i<args.size(); i++) {
                    append(args.get(i));
                    if(i < (args.size()-1)) {
                        this.filter.append(Tokens.SPACE);
                        this.filter.append(infixFunctions.get(name));
                        this.filter.append(Tokens.SPACE);
                    }
                }
            }
            this.filter.append(Tokens.RPAREN);
        }
        else {
            FunctionMethod method = obj.getMetadataObject();
            if (name.startsWith(method.getCategory())) {
                name = name.substring(method.getCategory().length()+1);
            }
            this.filter.append(name)
                  .append(Tokens.LPAREN);
            if (args != null && args.size() != 0) {
                if (SourceSystemFunctions.ENDSWITH.equalsIgnoreCase(name)) {
                    append(args.get(1));
                    this.filter.append(Tokens.COMMA);
                    append(args.get(0));
                } else {
                    for (int i = 0; i < args.size(); i++) {
                        append(args.get(i));
                        if (i < args.size()-1) {
                            this.filter.append(Tokens.COMMA);
                        }
                    }
                }
            }
            this.filter.append(Tokens.RPAREN);
        }
    }

    @Override
    public void visit(NamedTable obj) {
        this.entities.addEntity(obj.getMetadataObject());
    }

    @Override
    public void visit(IsNull obj) {
        if (obj.isNegated()) {
            this.filter.append(NOT.toLowerCase()).append(Tokens.LPAREN);
        }
        appendNested(obj.getExpression());
        this.filter.append(Tokens.SPACE);
        this.filter.append("eq").append(Tokens.SPACE); //$NON-NLS-1$
        this.filter.append(NULL.toLowerCase());
        if (obj.isNegated()) {
            this.filter.append(Tokens.RPAREN);
        }
    }

    private void appendNested(Expression ex) {
        boolean useParens = ex instanceof Condition;
        if (useParens) {
            this.filter.append(Tokens.LPAREN);
        }
        append(ex);
        if (useParens) {
            this.filter.append(Tokens.RPAREN);
        }
    }

    @Override
    public void visit(Join obj) {
        // joins are not used currently
        if (obj.getLeftItem() instanceof NamedTable && obj.getRightItem() instanceof NamedTable) {
            this.entities.addEntity(((NamedTable)obj.getLeftItem()).getMetadataObject());
            this.entities.addEntity(((NamedTable)obj.getRightItem()).getMetadataObject());
            obj.setCondition(buildEntityKey(obj.getCondition()));
            visitNode(obj.getCondition());
        }
        else {
            visitNode(obj.getLeftItem());
            visitNode(obj.getRightItem());
            visitNode(obj.getCondition());
        }
    }

    @Override
    public void visit(Limit obj) {
        if (obj.getRowOffset() != 0) {
            this.skip = new Integer(obj.getRowOffset());
        }
        if (obj.getRowLimit() != 0) {
            this.top = new Integer(obj.getRowLimit());
        }
    }

    @Override
    public void visit(Literal obj) {
        this.executionFactory.convertToODataInput(obj, this.filter);
    }

    @Override
    public void visit(Not obj) {
        this.filter.append(NOT)
        .append(Tokens.SPACE)
        .append(Tokens.LPAREN);
        append(obj.getCriteria());
        this.filter.append(Tokens.RPAREN);
    }

    @Override
    public void visit(OrderBy obj) {
         append(obj.getSortSpecifications());
    }

    @Override
    public void visit(SortSpecification obj) {
        if (this.orderBy.length() > 0) {
            this.orderBy.append(Tokens.COMMA);
        }
        ColumnReference column = (ColumnReference)obj.getExpression();
        this.orderBy.append(column.getMetadataObject().getName());
        // default is ascending
        if (obj.getOrdering() == Ordering.DESC) {
            this.orderBy.append(Tokens.SPACE).append(DESC.toLowerCase());
        }
    }

    @Override
    public void visit(Select obj) {
        visitNodes(obj.getFrom());
        obj.setWhere(buildEntityKey(obj.getWhere()));
        visitNode(obj.getWhere());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
        visitNodes(obj.getDerivedColumns());
    }

    protected Condition buildEntityKey(Condition obj) {
        List<Condition> crits = LanguageUtil.separateCriteriaByAnd(obj);
        if (!crits.isEmpty()) {
            boolean modified = false;
            for(Iterator<Condition> iter = crits.iterator(); iter.hasNext();) {
                Condition crit = iter.next();
                if (crit instanceof Comparison) {
                    Comparison left = (Comparison) crit;
                    boolean leftAdded = this.entities.addEnityKey(left);
                    if (leftAdded) {
                        iter.remove();
                        modified = true;
                    }
                }
            }
            if (this.entities.valid() && modified) {
                return LanguageUtil.combineCriteria(crits);
            }
        }
        return obj;
    }

    @Override
    public void visit(DerivedColumn obj) {
        if (obj.getExpression() instanceof ColumnReference) {
            Column column = ((ColumnReference)obj.getExpression()).getMetadataObject();
            String joinColumn = column.getProperty(ODataMetadataProcessor.JOIN_COLUMN, false);
            if (joinColumn != null && Boolean.valueOf(joinColumn)) {
                this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17006, column.getName())));
            }
            this.selectColumns.add(column);
        }
        else if (obj.getExpression() instanceof AggregateFunction) {
            AggregateFunction func = (AggregateFunction)obj.getExpression();
            if (func.getName().equalsIgnoreCase("COUNT")) { //$NON-NLS-1$
                this.count = true;
            }
            else {
                this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17007, func.getName())));
            }
        }
        else {
            this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17008)));
        }
    }

    private String getColumnName(Column column) {
        String columnName = column.getName();
        // Check if this is a embedded column, if it is then only
        // add the parent type
        String columnGroup = column.getProperty(ODataMetadataProcessor.COLUMN_GROUP, false);
        if (columnGroup != null) {
            columnName = columnGroup;
        }
        return columnName;
    }

    public void append(LanguageObject obj) {
        visitNode(obj);
    }

    protected void append(List<? extends LanguageObject> items) {
        if (items != null && items.size() != 0) {
            for (int i = 0; i < items.size(); i++) {
                append(items.get(i));
            }
        }
    }

    protected void append(LanguageObject[] items) {
        if (items != null && items.length != 0) {
            for (int i = 0; i < items.length; i++) {
                append(items[i]);
            }
        }
    }

    static class Entity {
        Table table;
        Map<Column, Literal> pkValues = new LinkedHashMap<Column, Literal>();
        boolean hasValidKey = false;
        List<Object[]> relations = new ArrayList<Object[]>();

        public Entity(Table t) {
            this.table = t;
        }

        public void addKeyValue(Column column, Literal value) {
            addKeyValue(column, value, true);
        }

        private void addKeyValue(Column column, Literal value, boolean walkRelations) {
            // add in self key
            this.pkValues.put(column, value);

            if (walkRelations) {
                // See any other relations exist.
                for (Object[] relation:this.relations) {
                    if (column.equals(relation[0])){
                        ((Entity)relation[2]).addKeyValue((Column)relation[1], value, false);
                    }
                }
            }

            for (Column col:this.table.getPrimaryKey().getColumns()) {
                if (this.pkValues.get(col) == null) {
                    return;
                }
            }
            this.hasValidKey = true;
        }

        public boolean hasValidKey() {
            return this.hasValidKey;
        }

        public void addRelation(Column self, Column other, Entity otherEntity) {
            this.relations.add(new Object[] {self, other, otherEntity});
        }
    }

    class EntitiesInQuery {
        ArrayList<Entity> entities = new ArrayList<ODataSQLVisitor.Entity>();

        public void append(StringBuilder url) {
            if (this.entities.size() == 1) {
                addEntityToURL(url, this.entities.get(0));
            }
            else if (this.entities.size() > 1) {
                for (int i = 0; i < this.entities.size()-1; i++) {
                    addEntityToURL(url, this.entities.get(i));
                    url.append("/"); //$NON-NLS-1$
                }
                addEntityToURL(url, this.entities.get(this.entities.size()-1));
            }
        }

        public boolean isKeyLookup() {
            return this.entities.get(this.entities.size()-1).hasValidKey();
        }

        public Table getFinalEntity() {
            return this.entities.get(this.entities.size()-1).table;
        }

        private void addEntityToURL(StringBuilder url, Entity entity) {
            url.append(entity.table.getName());
            if (entity.hasValidKey()) {
                boolean useNames = entity.pkValues.size() > 1; // multi-key PK, use the name value pairs
                url.append("("); //$NON-NLS-1$
                boolean firstKey = true;
                for (Column c:entity.pkValues.keySet()) {
                    if (firstKey) {
                        firstKey = false;
                    }
                    else {
                        url.append(Tokens.COMMA);
                    }
                    if (useNames) {
                        url.append(c.getName()).append(Tokens.EQ);
                    }
                    ODataSQLVisitor.this.executionFactory.convertToODataInput(entity.pkValues.get(c), url);
                }
                url.append(")"); //$NON-NLS-1$
            }
        }

        public void addEntity(Table table) {
            Entity entity = new Entity(table);
            this.entities.add(entity);
        }

        private Entity getEntity(Table table) {
            for (Entity e:this.entities) {
                if (e.table.equals(table)) {
                    return e;
                }
            }
            return null;
        }

        private boolean addEnityKey(Comparison obj) {
            if (obj.getOperator().equals(Comparison.Operator.EQ)) {
                if (obj.getLeftExpression() instanceof ColumnReference && obj.getRightExpression() instanceof Literal) {
                    ColumnReference columnRef = (ColumnReference)obj.getLeftExpression();
                    Table parentTable = columnRef.getTable().getMetadataObject();
                    Entity entity = getEntity(parentTable);
                    if (entity != null) {
                        Column column = columnRef.getMetadataObject();
                        if (parentTable.getPrimaryKey().getColumnByName(column.getName())!=null) {
                            entity.addKeyValue(column, (Literal)obj.getRightExpression());
                            return true;
                        }
                    }
                }
                if (obj.getLeftExpression() instanceof ColumnReference && obj.getRightExpression() instanceof ColumnReference) {
                    Column left = ((ColumnReference)obj.getLeftExpression()).getMetadataObject();
                    Column right = ((ColumnReference)obj.getRightExpression()).getMetadataObject();

                    if (isJoinOrPkColumn(left)&& isJoinOrPkColumn(right)) {
                        // in odata the navigation from parent to child implicit by their keys
                        Entity leftEntity = getEntity((Table)left.getParent());
                        Entity rightEntity = getEntity((Table)right.getParent());
                        leftEntity.addRelation(left, right, rightEntity);
                        rightEntity.addRelation(right,left, leftEntity);
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean isJoinOrPkColumn(Column column) {
            boolean joinColumn = Boolean.valueOf(column.getProperty(ODataMetadataProcessor.JOIN_COLUMN, false));
            if (!joinColumn) {
                Table table = (Table)column.getParent();
                return (table.getPrimaryKey().getColumnByName(column.getName()) != null);

            }
            return false;
        }

        private boolean valid() {
            for (Entity e:this.entities) {
                if (e.hasValidKey()) {
                    return true;
                }
            }
            return false;
        }
    }
}
