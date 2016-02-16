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

package org.teiid.translator.cassandra;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class TestCassandraQueryExecution {

	@Test public void testGetRowWithNull() {
		CassandraQueryExecution cqe = new CassandraQueryExecution(null, null, null);
		Row row = Mockito.mock(Row.class);
        Mockito.stub(row.isNull(0)).toReturn(true);
        ColumnDefinitions cd = Mockito.mock(ColumnDefinitions.class);
        Mockito.stub(row.getColumnDefinitions()).toReturn(cd);
        Mockito.stub(cd.size()).toReturn(1);
        Mockito.stub(cd.getType(0)).toReturn(DataType.cint());
        List<?> val = cqe.getRow(row);
        assertNull(val.get(0));
	}
	
}
