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
package org.teiid.olingo.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.data.Entity;
import org.teiid.odata.api.QueryResponse;
import org.teiid.olingo.ComplexReturnType;

public class CrossJoinResult implements QueryResponse {
    private final String invalidCharacterReplacement;
    private String nextToken;
    private CrossJoinResource resource;
    private List<List<ComplexReturnType>> out = new ArrayList<List<ComplexReturnType>>();
    
    public CrossJoinResult(String invalidCharacterReplacement, CrossJoinResource context) {
        this.invalidCharacterReplacement = invalidCharacterReplacement;
        this.resource = context;
    }

    @Override
    public void addRow(ResultSet rs) throws SQLException {

        ArrayList<ComplexReturnType> row = new ArrayList<ComplexReturnType>();
        
        Entity entity = EntityList.createEntity(rs, this.resource.getEdmEntityType(),
                this.resource.getAllProjectedColumns(),
                this.invalidCharacterReplacement);
        row.add(new ComplexReturnType(this.resource.getTable().getName(), this.resource.getEdmEntityType(), entity, this.resource.hasExpand()));
        for (EntityResource er:this.resource.getSibilings()) {
            Entity sibiling = EntityList.createEntity(rs, er.getEdmEntityType(),
                    er.getAllProjectedColumns(),
                    this.invalidCharacterReplacement);
            row.add(new ComplexReturnType(er.getTable().getName(), this.resource.getEdmEntityType(), sibiling, ((CrossJoinResource)er).hasExpand()));
        }
        this.out.add(row);
    }
    
    public CrossJoinResource getResource() {
        return this.resource;
    }

    public List<List<ComplexReturnType>> getResults(){
        return this.out;
    }

    @Override
    public long size() {
        return this.out.size();
    }

    @Override
    public void setCount(long count) {
    }

    @Override
    public void setNextToken(String token) {
        this.nextToken = token;
    }

    @Override
    public String getNextToken() {
        return this.nextToken;
    }
}
