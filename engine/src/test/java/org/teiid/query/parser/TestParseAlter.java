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
        alterTrigger.setDefinition((TriggerAction) QueryParser.getQueryParser().parseProcedure("for each row begin end", true));
        helpTest("alter trigger on x instead of update as for each row begin end", "ALTER TRIGGER ON x INSTEAD OF UPDATE AS\nFOR EACH ROW\nBEGIN ATOMIC\nEND", alterTrigger);
    }

    @Test public void testAlterDisabled() throws Exception {
        AlterTrigger alterTrigger = new AlterTrigger();
        alterTrigger.setTarget(new GroupSymbol("x"));
        alterTrigger.setEvent(TriggerEvent.UPDATE);
        alterTrigger.setEnabled(false);
        helpTest("alter trigger on x instead of update disabled", "ALTER TRIGGER ON x INSTEAD OF UPDATE DISABLED", alterTrigger);
    }

    @Test public void testAlterDisabledPhysical() throws Exception {
        AlterTrigger alterTrigger = new AlterTrigger();
        alterTrigger.setTarget(new GroupSymbol("x"));
        alterTrigger.setEvent(TriggerEvent.UPDATE);
        alterTrigger.setEnabled(false);
        alterTrigger.setAfter(true);
        alterTrigger.setName("y");
        helpTest("alter trigger y on x after update disabled", "ALTER TRIGGER y ON x AFTER UPDATE DISABLED", alterTrigger);
    }

    @Test public void testCreateTrigger() throws Exception {
        AlterTrigger alterTrigger = new AlterTrigger();
        alterTrigger.setCreate(true);
        alterTrigger.setTarget(new GroupSymbol("x"));
        alterTrigger.setEvent(TriggerEvent.UPDATE);
        alterTrigger.setDefinition((TriggerAction) QueryParser.getQueryParser().parseProcedure("for each row begin end", true));
        helpTest("create trigger on x instead of update as for each row begin end", "CREATE TRIGGER ON x INSTEAD OF UPDATE AS\nFOR EACH ROW\nBEGIN ATOMIC\nEND", alterTrigger);
    }

    @Test public void testCreateTriggerPhysical() throws Exception {
        AlterTrigger alterTrigger = new AlterTrigger();
        alterTrigger.setCreate(true);
        alterTrigger.setTarget(new GroupSymbol("x"));
        alterTrigger.setEvent(TriggerEvent.INSERT);
        alterTrigger.setAfter(true);
        alterTrigger.setName("z");
        alterTrigger.setDefinition((TriggerAction) QueryParser.getQueryParser().parseProcedure("for each row begin end", true));
        helpTest("create trigger z on x after insert as for each row begin end", "CREATE TRIGGER z ON x AFTER INSERT AS\nFOR EACH ROW\nBEGIN ATOMIC\nEND", alterTrigger);
    }

}
