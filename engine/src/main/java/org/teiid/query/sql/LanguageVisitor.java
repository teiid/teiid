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

package org.teiid.query.sql;

import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.proc.*;
import org.teiid.query.sql.symbol.*;

/**
 * <p>The LanguageVisitor can be used to visit a LanguageObject as if it were a tree
 * and perform some action on some or all of the language objects that are visited.
 * The LanguageVisitor is extended to create a concrete visitor and some or all of
 * the public visit methods should be overridden to provide the visitor functionality.
 * These public visit methods SHOULD NOT be called directly.
 */
@SuppressWarnings("unused")
public abstract class LanguageVisitor {

    private boolean abort = false;

    public void setAbort(boolean abort) {
        this.abort = abort;
    }

    public final boolean shouldAbort() {
        return abort;
    }

    // Visitor methods for language objects
    public void visit(BatchedUpdateCommand obj) {}
    public void visit(BetweenCriteria obj) {}
    public void visit(CaseExpression obj) {}
    public void visit(CompareCriteria obj) {}
    public void visit(CompoundCriteria obj) {}
    public void visit(Delete obj) {
        visit((ProcedureContainer)obj);
    }
    public void visit(ExistsCriteria obj) {}
    public void visit(From obj) {}
    public void visit(GroupBy obj) {}
    public void visit(Insert obj) {
        visit((ProcedureContainer)obj);
    }
    public void visit(IsNullCriteria obj) {}
    public void visit(JoinPredicate obj) {}
    public void visit(JoinType obj) {}
    public void visit(Limit obj) {}
    public void visit(MatchCriteria obj) {}
    public void visit(NotCriteria obj) {}
    public void visit(Option obj) {}
    public void visit(OrderBy obj) {}
    public void visit(Query obj) {}
    public void visit(SearchedCaseExpression obj) {}
    public void visit(Select obj) {}
    public void visit(SetCriteria obj) {}
    public void visit(SetQuery obj) {}
    public void visit(StoredProcedure obj) {
        visit((ProcedureContainer)obj);
    }
    public void visit(SubqueryCompareCriteria obj) {}
    public void visit(SubqueryFromClause obj) {}
    public void visit(SubquerySetCriteria obj) {}
    public void visit(UnaryFromClause obj) {}
    public void visit(Update obj) {
        visit((ProcedureContainer)obj);
    }
    public void visit(Into obj) {}
    public void visit(DependentSetCriteria obj) {}
    public void visit(Create obj) {}
    public void visit(Drop obj) {}

    // Visitor methods for symbol objects
    public void visit(AggregateSymbol obj) {}
    public void visit(AliasSymbol obj) {}
    public void visit(MultipleElementSymbol obj) {}
    public void visit(Constant obj) {}
    public void visit(ElementSymbol obj) {}
    public void visit(ExpressionSymbol obj) {}
    public void visit(Function obj) {}
    public void visit(GroupSymbol obj) {}
    public void visit(Reference obj) {}
    public void visit(ScalarSubquery obj) {}

    // Visitor methods for procedure language objects
    public void visit(AssignmentStatement obj) {}
    public void visit(Block obj) {}
    public void visit(CommandStatement obj) {}
    public void visit(CreateProcedureCommand obj) {}
    public void visit(DeclareStatement obj) {
        visit((AssignmentStatement)obj);
    }
    public void visit(IfStatement obj) {}
    public void visit(RaiseStatement obj) {}
    public void visit(BranchingStatement obj) {}
    public void visit(WhileStatement obj) {}
    public void visit(LoopStatement obj) {}
    public void visit(DynamicCommand obj) {}
    public void visit(ProcedureContainer obj) {}
    public void visit(SetClauseList obj) {}
    public void visit(SetClause obj) {}
    public void visit(OrderByItem obj) {}
    public void visit(XMLElement obj) {}
    public void visit(XMLAttributes obj) {}
    public void visit(XMLForest obj) {}
    public void visit(XMLNamespaces obj) {}
    public void visit(TextTable obj) {}
    public void visit(TextLine obj) {}
    public void visit(XMLTable obj) {}
    public void visit(DerivedColumn obj) {}
    public void visit(XMLSerialize obj) {}
    public void visit(XMLQuery obj) {}
    public void visit(QueryString obj) {}
    public void visit(XMLParse obj) {}
    public void visit(ExpressionCriteria obj) {}
    public void visit(WithQueryCommand obj) {}
    public void visit(TriggerAction obj) {}
    public void visit(ArrayTable obj) {}

    public void visit(AlterView obj) {}
    public void visit(AlterProcedure obj) {}
    public void visit(AlterTrigger obj) {}

    public void visit(WindowFunction windowFunction) {}
    public void visit(WindowSpecification windowSpecification) {}
    public void visit(WindowFrame windowFrame) {}

    public void visit(Array array) {}
    public void visit(ObjectTable objectTable) {}

    public void visit(ExceptionExpression obj) {}

    public void visit(ReturnStatement obj) {}

    public void visit(JSONObject obj) {}

    public void visit(XMLExists xmlExists) {}

    public void visit(XMLCast xmlCast) {}

    public void visit(IsDistinctCriteria isDistinctCriteria) {}

    public void visit(JsonTable jsonTable) {}

    public void visit(ExplainCommand explainCommand) {}
}
