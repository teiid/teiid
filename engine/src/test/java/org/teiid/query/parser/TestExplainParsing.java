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
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.ExplainCommand;
import org.teiid.query.sql.lang.ExplainCommand.Format;

@SuppressWarnings("nls")
public class TestExplainParsing {

    @Test public void testExplainQuery() throws Exception {
        ExplainCommand explain = new ExplainCommand();
        explain.setFormat(Format.TEXT);
        explain.setAnalyze(true);
        explain.setCommand(QueryParser.getQueryParser().parseCommand("/*+ cache */ SELECT 1"));
        helpTest("EXPLAIN (ANALYZE true, FORMAT TEXT) /*+ cache */ SELECT 1", "EXPLAIN (ANALYZE true, FORMAT TEXT) /*+ cache */ SELECT 1", explain);
    }

    @Test public void testExplainProc() throws Exception {
        ExplainCommand explain = new ExplainCommand();
        explain.setFormat(Format.YAML);
        explain.setCommand(QueryParser.getQueryParser().parseCommand("begin SELECT 1; end"));
        helpTest("EXPLAIN (FORMAT yaml) begin SELECT 1; end", "EXPLAIN (FORMAT YAML) BEGIN\n" +
                "SELECT 1;\n" +
                "END", explain);
    }

    @Test public void testExplainAnalyzeDefault() throws Exception {
        ExplainCommand explain = new ExplainCommand();
        explain.setAnalyze(true);
        explain.setCommand(QueryParser.getQueryParser().parseCommand("/*+ cache */ SELECT 1"));
        helpTest("EXPLAIN (ANALYZE) /*+ cache */ SELECT 1", "EXPLAIN (ANALYZE true) /*+ cache */ SELECT 1", explain);
    }

    @Test public void testEquality() throws Exception {
        ExplainCommand explain = new ExplainCommand();
        explain.setAnalyze(true);
        explain.setCommand(QueryParser.getQueryParser().parseCommand("select * from x"));
        ExplainCommand clone = explain.clone();

        UnitTestUtil.helpTestEquivalence(0, explain, clone);

        explain.setFormat(Format.YAML);
        UnitTestUtil.helpTestEquivalence(1, explain, clone);
    }

}
