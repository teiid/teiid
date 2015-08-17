/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Select;
import org.teiid.language.SetClause;
import org.teiid.language.TableReference;
import org.teiid.language.Update;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;


public class HBaseSQLConversionVisitor extends org.teiid.translator.jdbc.SQLConversionVisitor {
    
    private HBaseExecutionFactory executionFactory ;
    
    public HBaseSQLConversionVisitor(HBaseExecutionFactory ef) {
        super(ef);
        this.executionFactory = ef;
    }
    
    @Override
    protected String getInsertKeyword() {
    	return "UPSERT"; //$NON-NLS-1$
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
