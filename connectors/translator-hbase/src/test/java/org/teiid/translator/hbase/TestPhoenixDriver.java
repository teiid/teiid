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

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.translator.hbase.phoenix.PhoenixUtils;

@SuppressWarnings("nls")
public class TestPhoenixDriver {
    
    @Test
    public void testHBaseTableMapping() throws QueryMetadataException, TeiidComponentException {
        
        String expect = "CREATE TABLE IF NOT EXISTS Customer (ROW_ID VARCHAR PRIMARY KEY, customer.city VARCHAR, customer.name VARCHAR, sales.amount VARCHAR, sales.product VARCHAR)";

        assertEquals(expect, PhoenixUtils.hbaseTableMappingDDL(TestHBaseUtil.queryMetadataInterface().getModelID("HBaseModel").getTable("Customer")));
        
        expect = "CREATE TABLE IF NOT EXISTS TypesTest (ROW_ID VARCHAR PRIMARY KEY, f.column1 VARCHAR, f.column2 VARBINARY, f.column3 CHAR, f.column4 BOOLEAN, f.column5 TINYINT, f.column6 TINYINT, f.column7 SMALLINT, f.column8 SMALLINT, f.column9 INTEGER, f.column10 INTEGER, f.column11 LONG, f.column12 LONG, f.column13 FLOAT, f.column14 FLOAT, f.column15 DOUBLE, f.column16 DECIMAL, f.column17 DECIMAL, f.column18 DATE, f.column19 TIME, f.column20 TIMESTAMP)";

        assertEquals(expect, PhoenixUtils.hbaseTableMappingDDL(TestHBaseUtil.queryMetadataInterface().getModelID("HBaseModel").getTable("TypesTest")));
    
    }
    
}
