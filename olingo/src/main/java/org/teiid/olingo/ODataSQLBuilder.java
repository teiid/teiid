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
package org.teiid.olingo;

import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.queryoption.expression.Alias;
import org.apache.olingo.server.api.uri.queryoption.expression.Binary;
import org.apache.olingo.server.api.uri.queryoption.expression.Enumeration;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitor;
import org.apache.olingo.server.api.uri.queryoption.expression.LambdaRef;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.expression.Method;
import org.apache.olingo.server.api.uri.queryoption.expression.TypeLiteral;
import org.apache.olingo.server.api.uri.queryoption.expression.Unary;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

public class ODataSQLBuilder implements ExpressionVisitor {
	private MetadataStore metadata;
	private boolean prepared = true;
	private List<ProjectedColumn> projectedColumns = new ArrayList<ProjectedColumn>();
	private ArrayList<SQLParam> params = new ArrayList<SQLParam>();
		
	public ODataSQLBuilder(MetadataStore metadata, boolean prepared) {
		this.metadata = metadata;
		this.prepared = prepared;
	}
	
	public Query selectString(String entityName, UriInfo info) {
		UriInfoResource queryInfo = info.asUriInfoResource();
		Query query = new Query();
		
		Table table = findTable(entityName, this.metadata);
		GroupSymbol group = new GroupSymbol("g0", entityName); //$NON-NLS-1$
		UnaryFromClause fromCluse = new UnaryFromClause(group);
		
		Select select = new Select();				
		if (queryInfo.getSelectOption() != null) {
			
		}
		else {
			for (final Column column:table.getColumns()) {
				select.addSymbol(new ElementSymbol(column.getName(), group));
				this.projectedColumns.add(new ProjectedColumn() {
					@Override
					public String getName() {
						return column.getName();
					}
					@Override
					public boolean isVisible() {
						return true;
					}
				});
			}
		}
		
		From from = new From();
		from.addClause(fromCluse);
		query.setSelect(select);
		query.setFrom(from);
		//query.setCriteria(this.criteria);
		return query;		
	}
	
	List<ProjectedColumn> getProjectedColumns(){
		return this.projectedColumns;
	}
	
	private Table findTable(String tableName, MetadataStore store) {
		for (Schema s : store.getSchemaList()) {
			for (Table t : s.getTables().values()) {
				if (t.getFullName().equals(tableName)) {
					return t;
				}
			}
		}
		for (Schema s : store.getSchemaList()) {
			for (Table t : s.getTables().values()) {
				if (t.getName().equals(tableName)) {
					return t;
				}
			}
		}		
		return null;
	}

	private Column findColumn(Table table, String propertyName) {
		return table.getColumnByName(propertyName);
	}
	
	public List<SQLParam> getParameters(){
		return this.params;
	}
	
	@Override
	public void visit(Binary obj) throws ExpressionVisitException,
			ODataApplicationException {
		// rameshTODO Auto-generated method stub

	}

	@Override
	public void visit(Unary obj) throws ExpressionVisitException,
			ODataApplicationException {
		// rameshTODO Auto-generated method stub

	}

	@Override
	public void visit(Method obj) throws ExpressionVisitException,
			ODataApplicationException {
		// rameshTODO Auto-generated method stub

	}

	@Override
	public void visit(LambdaRef obj) throws ExpressionVisitException,
			ODataApplicationException {
		// rameshTODO Auto-generated method stub

	}

	@Override
	public void visit(Literal obj) throws ExpressionVisitException,
			ODataApplicationException {
		// rameshTODO Auto-generated method stub

	}

	@Override
	public void visit(Member obj) throws ExpressionVisitException,
			ODataApplicationException {
		// rameshTODO Auto-generated method stub

	}

	@Override
	public void visit(Alias obj) throws ExpressionVisitException,
			ODataApplicationException {
		// rameshTODO Auto-generated method stub

	}

	@Override
	public void visit(TypeLiteral obj) throws ExpressionVisitException,
			ODataApplicationException {
		// rameshTODO Auto-generated method stub

	}

	@Override
	public void visit(Enumeration obj) throws ExpressionVisitException,
			ODataApplicationException {
		// rameshTODO Auto-generated method stub

	}

}
