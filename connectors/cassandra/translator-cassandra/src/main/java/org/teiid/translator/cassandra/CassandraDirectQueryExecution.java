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

package org.teiid.translator.cassandra;

import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

public class CassandraDirectQueryExecution extends CassandraQueryExecution implements ProcedureExecution {

    private String cql;
    private List<Argument> arguments;

    public CassandraDirectQueryExecution(String cql, List<Argument> arguments, Command command, CassandraConnection connection, ExecutionContext context, boolean returnsArray){
        super(command, connection, context);
        this.arguments = arguments;
        this.cql = cql;
        this.returnsArray = returnsArray;
    }

    @Override
    public void execute() throws TranslatorException {
        StringBuilder buffer = new StringBuilder();
        SQLStringVisitor.parseNativeQueryParts(cql, arguments, buffer, new SQLStringVisitor.Substitutor() {

            @Override
            public void substitute(Argument arg, StringBuilder builder, int index) {
                Literal argumentValue = arg.getArgumentValue();
                builder.append(argumentValue);
            }
        });
        String source_cql = buffer.toString();
        execute(source_cql);
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return null;
    }

}
