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
package org.teiid.translator.phoenix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.language.*;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;


public class PhoenixSQLConversionVisitor extends org.teiid.translator.jdbc.SQLConversionVisitor {

    private PhoenixExecutionFactory executionFactory ;

    public PhoenixSQLConversionVisitor(PhoenixExecutionFactory ef) {
        super(ef);
        this.executionFactory = ef;
    }

    @Override
    protected String getInsertKeyword() {
        return "UPSERT"; //$NON-NLS-1$
    }

    @Override
    public void visit(Like obj) {
        obj.setEscapeCharacter(null); //not supported - capabilities ensure only \ is pushed
        super.visit(obj);
    }

    @Override
    public void visit(Update update) {
        //use an upsert
        List<ColumnReference> cols = new ArrayList<ColumnReference>();
        List<Expression> vals = new ArrayList<Expression>();
        for (SetClause set : update.getChanges()) {
            cols.add(set.getSymbol());
            vals.add(set.getValue());
        }
        Insert insert = null;
        if (update.getWhere() == null) {
            insert = new Insert(update.getTable(), cols, new ExpressionValueSource(vals));
        } else {
            List<DerivedColumn> select = new ArrayList<DerivedColumn>();
            Set<Column> columns = new HashSet<Column>();
            for (ColumnReference col : cols) {
                columns.add(col.getMetadataObject());
            }
            for (Expression val : vals) {
                select.add(new DerivedColumn(null, val));
            }

            KeyRecord pk = update.getTable().getMetadataObject().getPrimaryKey();
            if(pk != null) {
                for (Column c : pk.getColumns()) {
                    if (!columns.contains(c)) {
                        ColumnReference cr = new ColumnReference(update.getTable(), c.getName(), c, c.getJavaType());
                        select.add(new DerivedColumn(null, cr));
                        cols.add(cr);
                    }
                }
            }

            Select query = new Select(select, false, Arrays.asList((TableReference)update.getTable()), update.getWhere(), null, null, null);
            insert = new Insert(update.getTable(), cols, query);

        }
        append(insert);
    }

    public String getSQL(){
        return buffer.toString();
    }

}
