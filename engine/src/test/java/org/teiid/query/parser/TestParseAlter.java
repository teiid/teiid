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

import org.junit.Test;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.AlterView;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.symbol.GroupSymbol;

@SuppressWarnings("nls")
public class TestParseAlter {

	@Test public void testAlterView() throws Exception {
		AlterView alterView = new AlterView();
		alterView.setTarget(new GroupSymbol("x"));
		alterView.setDefinition((QueryCommand) QueryParser.getQueryParser().parseCommand("/*+ cache */ SELECT 1"));
		helpTest("alter view x as /*+ cache */ select 1", "ALTER VIEW x AS\n/*+ cache */ SELECT 1", alterView);
	}
	
	@Test public void testAlterProc() throws Exception {
		AlterView alterView = new AlterView();
		alterView.setTarget(new GroupSymbol("x"));
		alterView.setDefinition((QueryCommand) QueryParser.getQueryParser().parseCommand("/*+ cache */ SELECT 1"));
		helpTest("alter view x as /*+ cache */ select 1", "ALTER VIEW x AS\n/*+ cache */ SELECT 1", alterView);
	}
	
	@Test public void testAlterTrigger() throws Exception {
		AlterTrigger alterTrigger = new AlterTrigger();
		alterTrigger.setTarget(new GroupSymbol("x"));
		alterTrigger.setEvent(TriggerEvent.UPDATE);
		alterTrigger.setDefinition((TriggerAction) QueryParser.getQueryParser().parseUpdateProcedure("for each row begin end"));
		helpTest("alter trigger on x instead of update as for each row begin end", "ALTER TRIGGER ON x INSTEAD OF UPDATE AS\nFOR EACH ROW\nBEGIN ATOMIC\nEND", alterTrigger);
	}
	
	@Test public void testAlterDisabled() throws Exception {
		AlterTrigger alterTrigger = new AlterTrigger();
		alterTrigger.setTarget(new GroupSymbol("x"));
		alterTrigger.setEvent(TriggerEvent.UPDATE);
		alterTrigger.setEnabled(false);
		helpTest("alter trigger on x instead of update disabled", "ALTER TRIGGER ON x INSTEAD OF UPDATE DISABLED", alterTrigger);
	}
	
	@Test public void testCreateTrigger() throws Exception {
		AlterTrigger alterTrigger = new AlterTrigger();
		alterTrigger.setCreate(true);
		alterTrigger.setTarget(new GroupSymbol("x"));
		alterTrigger.setEvent(TriggerEvent.UPDATE);
		alterTrigger.setDefinition((TriggerAction) QueryParser.getQueryParser().parseUpdateProcedure("for each row begin end"));
		helpTest("create trigger on x instead of update as for each row begin end", "CREATE TRIGGER ON x INSTEAD OF UPDATE AS\nFOR EACH ROW\nBEGIN ATOMIC\nEND", alterTrigger);
	}
	
}
