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

package org.teiid.query.parser;

import static org.teiid.query.parser.TestParser.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

@SuppressWarnings("nls")
public class TestCreateDrop {

    @Test public void testCreateTempTable1() {
        Create create = new Create();
        create.setTable(new GroupSymbol("tempTable")); //$NON-NLS-1$
        List<ElementSymbol> columns = new ArrayList<ElementSymbol>();
        ElementSymbol column = new ElementSymbol("c1");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
        columns.add(column);
        column = new ElementSymbol("c2");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BYTE);
        columns.add(column);
        create.setElementSymbolsAsColumns(columns);
        helpTest("Create local TEMPORARY table tempTable (c1 boolean, c2 byte)", "CREATE LOCAL TEMPORARY TABLE tempTable (c1 boolean, c2 byte)", create); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testCreateTempTable2() {
        Create create = new Create();
        create.setTable(new GroupSymbol("tempTable")); //$NON-NLS-1$
        List<ElementSymbol> columns = new ArrayList<ElementSymbol>();
        ElementSymbol column = new ElementSymbol("c1");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
        columns.add(column);
        column = new ElementSymbol("c2");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BYTE);
        columns.add(column);
        create.setElementSymbolsAsColumns(columns);
        helpTest("Create local TEMPORARY table tempTable(c1 boolean, c2 byte)", "CREATE LOCAL TEMPORARY TABLE tempTable (c1 boolean, c2 byte)", create); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testCreateTempTable3() {
        helpException("Create TEMPORARY table tempTable (c1 boolean, c2 byte)"); //$NON-NLS-1$ 
    }
    
    @Test public void testCreateTempTable4() {
        helpException("Create table tempTable (c1 boolean, c2 byte)"); //$NON-NLS-1$ 
    }
    
    @Test public void testCreateTempTable5() {
        helpException("Create  local TEMPORARY table tempTable (c1 boolean primary, c2 byte)"); //$NON-NLS-1$ 
    }
        
    @Test public void testCreateTempTable7() {
        helpException("Create local TEMPORARY table tempTable (c1.x boolean, c2 byte)" ,"TEIID31100 Parsing error: Encountered \"table tempTable ([*]c1.x[*] boolean,\" at line 1, column 41.\nInvalid simple identifier format: [c1.x]"); //$NON-NLS-1$ //$NON-NLS-2$ 
    }
    
    @Test public void testCreateTempTableWithPrimaryKey() {
        Create create = new Create();
        create.setTable(new GroupSymbol("tempTable")); //$NON-NLS-1$
        List<ElementSymbol> columns = new ArrayList<ElementSymbol>();
        ElementSymbol column = new ElementSymbol("c1");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
        columns.add(column);
        column = new ElementSymbol("c2");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BYTE);
        columns.add(column);
        create.setElementSymbolsAsColumns(columns);
        create.getPrimaryKey().add(column);
        helpTest("Create local TEMPORARY table tempTable(c1 boolean, c2 byte, primary key (c2))", "CREATE LOCAL TEMPORARY TABLE tempTable (c1 boolean, c2 byte, PRIMARY KEY(c2))", create); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testDropTable() {
        Drop drop = new Drop();
        drop.setTable(new GroupSymbol("tempTable")); //$NON-NLS-1$
        helpTest("DROP table tempTable", "DROP TABLE tempTable", drop); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testForeignTemp() {
        Create create = new Create();
        create.setTable(new GroupSymbol("tempTable")); //$NON-NLS-1$
        create.setOn("source");
        Table t = new Table();
        t.setName("tempTable");
        t.setUUID("tid:0");
        Column c = new Column();
        c.setName("x");
        c.setUUID("tid:0");
        Datatype string = SystemMetadata.getInstance().getRuntimeTypeMap().get("string");
        c.setDatatype(string, true);
        t.addColumn(c);
        c = new Column();
        c.setName("y");
        c.setUUID("tid:0");
        Datatype decimal = SystemMetadata.getInstance().getRuntimeTypeMap().get("decimal");
        c.setDatatype(decimal, true);
        t.addColumn(c);
        t.setCardinality(10000);
        create.setTableMetadata(t);
        helpTest("create foreign temporary table tempTable (x string, y decimal) options (cardinality 10000) on source", "CREATE FOREIGN TEMPORARY TABLE tempTable (\n	x string,\n	y bigdecimal\n) OPTIONS (CARDINALITY 10000) ON 'source'", create); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testBadCreate() {
        helpException("create insert"); //$NON-NLS-1$
    }

    @Test public void testTypeAliases() {
        Create create = new Create();
        create.setTable(new GroupSymbol("tempTable")); //$NON-NLS-1$
        List<ElementSymbol> columns = new ArrayList<ElementSymbol>();
        ElementSymbol column = new ElementSymbol("c1");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.STRING);
        columns.add(column);
        column = new ElementSymbol("c2");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BYTE);
        columns.add(column);
        column = new ElementSymbol("c3");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.SHORT);
        columns.add(column);
        column = new ElementSymbol("c4");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.FLOAT);
        columns.add(column);
        column = new ElementSymbol("c5");//$NON-NLS-1$
        column.setType(DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
        columns.add(column);
        create.setElementSymbolsAsColumns(columns);
        helpTest("Create local TEMPORARY table tempTable (c1 varchar, c2 tinyint, c3 smallint, c4 real, c5 decimal)", "CREATE LOCAL TEMPORARY TABLE tempTable (c1 varchar, c2 tinyint, c3 smallint, c4 real, c5 decimal)", create); //$NON-NLS-1$ 
    }
	
}
