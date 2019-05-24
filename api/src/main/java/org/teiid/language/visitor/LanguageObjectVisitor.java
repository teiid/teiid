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

package org.teiid.language.visitor;

import org.teiid.language.*;

/**
 */
public interface LanguageObjectVisitor {
    public void visit(AggregateFunction obj);
    public void visit(BatchedUpdates obj);
    public void visit(ExpressionValueSource obj);
    public void visit(Comparison obj);
    public void visit(AndOr obj);
    public void visit(Delete obj);
    public void visit(ColumnReference obj);
    public void visit(Call obj);
    public void visit(Exists obj);
    public void visit(Function obj);
    public void visit(NamedTable obj);
    public void visit(GroupBy obj);
    public void visit(In obj);
    public void visit(DerivedTable obj);
    public void visit(Insert obj);
    public void visit(IsNull obj);
    public void visit(Join obj);
    public void visit(Like obj);
    public void visit(Limit obj);
    public void visit(Literal obj);
    public void visit(Not obj);
    public void visit(OrderBy obj);
    public void visit(SortSpecification obj);
    public void visit(Argument obj);
    public void visit(Select obj);
    public void visit(ScalarSubquery obj);
    public void visit(SearchedCase obj);
    public void visit(DerivedColumn obj);
    public void visit(SubqueryComparison obj);
    public void visit(SubqueryIn obj);
    public void visit(Update obj);
    public void visit(SetQuery obj);
    public void visit(SetClause obj);
    public void visit(SearchedWhenClause obj);
    public void visit(With obj);
    public void visit(WithItem obj);
    public void visit(WindowFunction windowFunction);
    public void visit(WindowSpecification windowSpecification);
    public void visit(Parameter obj);
    public void visit(Array array);
    public void visit(NamedProcedureCall namedProcedureCall);
    public void visit(IsDistinct isDistinct);
    public void visit(WindowFrame windowFrame);
}
