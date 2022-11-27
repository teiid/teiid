package org.teiid.query.sql.lang;

import java.util.List;

import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class WithQueryCommand implements SubqueryContainer<QueryCommand> {

    public static final String NO_INLINE = "no_inline"; //$NON-NLS-1$
    public static final String MATERIALIZE = "materialize"; //$NON-NLS-1$

    private GroupSymbol groupSymbol;
    private List<ElementSymbol> columns;
    private QueryCommand queryExpression;
    private TupleBuffer tupleBuffer;
    private boolean recursive;
    private boolean noInline;
    private boolean materialize;

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
        WithQueryCommand clone = new WithQueryCommand(groupSymbol.clone(), LanguageObject.Util.deepClone(columns, ElementSymbol.class), null);
        if (queryExpression != null) {
            clone.queryExpression = (QueryCommand)queryExpression.clone();
        }
        clone.tupleBuffer = this.tupleBuffer;
        clone.recursive = recursive;
        clone.noInline = noInline;
        clone.materialize = materialize;
        return clone;
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
        EquivalenceUtil.areEqual(this.queryExpression, other.queryExpression) &&
        noInline == other.noInline &&
        materialize == other.materialize &&
        recursive == other.recursive;
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    public void setTupleBuffer(TupleBuffer tupleBuffer) {
        this.tupleBuffer = tupleBuffer;
    }

    public TupleBuffer getTupleBuffer() {
        return tupleBuffer;
    }

    public void setGroupSymbol(GroupSymbol groupSymbol) {
        this.groupSymbol = groupSymbol;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    public boolean isNoInline() {
        return noInline || materialize;
    }

    public void setNoInline(boolean noUnnest) {
        this.noInline = noUnnest;
    }

    public boolean isMaterialize() {
        return materialize;
    }

    public void setMaterialize(boolean materialize) {
        this.materialize = materialize;
    }

}
