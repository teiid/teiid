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

package org.teiid.query.sql.util;


import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.metadata.BasicQueryMetadataWrapper;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestElementSymbolOptimizer {

    public Command helpResolve(String sql, QueryMetadataInterface metadata) throws QueryParserException, QueryResolverException, TeiidComponentException {
        Command command = QueryParser.getQueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);

        return command;
    }

    public void helpTestOptimize(String sql, QueryMetadataInterface metadata, String expected) throws QueryMetadataException, TeiidComponentException, QueryParserException, QueryResolverException {
        Command command = helpResolve(sql, new BasicQueryMetadataWrapper(metadata){
            @Override
            public boolean findShortName() {
                return true;
            }
        });
        String actual = command.toString();

        assertEquals("Expected different optimized string", expected, actual);             //$NON-NLS-1$
    }

    /** Can be optimized */
    @Test public void testOptimize1() throws Exception {
        helpTestOptimize("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "SELECT e1, e2 FROM pm1.g1"); //$NON-NLS-1$
    }

    /** Can't be optimized */
    @Test public void testOptimize2() throws Exception {
        helpTestOptimize("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1, pm1.g2", //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1, pm1.g2"); //$NON-NLS-1$
    }

    @Test public void testOptimize3() throws Exception {
        helpTestOptimize("UPDATE pm1.g1 SET pm1.g1.e1 = 'e' WHERE pm1.g1.e2 = 3", //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "UPDATE pm1.g1 SET e1 = 'e' WHERE e2 = 3"); //$NON-NLS-1$
    }

    @Test public void testOptimize4() throws Exception {
        helpTestOptimize("INSERT INTO pm1.g1 (pm1.g1.e1, pm1.g1.e2) VALUES ('e', 3)", //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "INSERT INTO pm1.g1 (e1, e2) VALUES ('e', 3)"); //$NON-NLS-1$
    }

    @Test public void testOptimize5() throws Exception {
        helpTestOptimize("DELETE FROM pm1.g1 WHERE pm1.g1.e2 = 3", //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "DELETE FROM pm1.g1 WHERE e2 = 3"); //$NON-NLS-1$
    }

    @Test public void testOptimize6() throws Exception {
        helpTestOptimize("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 WHERE e2 > (SELECT AVG(pm1.g2.e2) FROM pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1)", //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "SELECT e1, e2 FROM pm1.g1 WHERE e2 > (SELECT AVG(e2) FROM pm1.g2 WHERE pm1.g1.e1 = e1)"); //$NON-NLS-1$
    }

    /** alias */
    @Test public void testOptimize7() throws Exception {
        helpTestOptimize("SELECT 'text' AS zz, pm1.g1.e2 FROM pm1.g1", //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "SELECT 'text' AS zz, e2 FROM pm1.g1"); //$NON-NLS-1$
    }

    @Test public void testOptimize8() throws Exception {
        helpTestOptimize("SELECT 1, 'xyz'", //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "SELECT 1, 'xyz'"); //$NON-NLS-1$
    }

    public void helpTestFullyQualify(String sql, QueryMetadataInterface metadata, String expected) throws QueryParserException, QueryResolverException, TeiidComponentException {
        Command command = helpResolve(sql, metadata);
        ResolverUtil.fullyQualifyElements(command);
        String actual = command.toString();

        assertEquals("Expected different fully qualified string", expected, actual); //$NON-NLS-1$
    }

    @Test public void testFullyQualify1() throws Exception {
        helpTestFullyQualify("SELECT e1, e2 FROM pm1.g1",  //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"); //$NON-NLS-1$
    }

    @Test public void testVirtualStoredProcedure() throws Exception {
        helpTestOptimize("EXEC pm1.vsp7(5)",  //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "EXEC pm1.vsp7(5)"); //$NON-NLS-1$
    }

    @Test public void testStoredQuerySubquery() throws Exception {
        helpTestOptimize("select x.e1 from (EXEC pm1.sq1()) as x",  //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "SELECT e1 FROM (EXEC pm1.sq1()) AS x"); //$NON-NLS-1$
    }

    @Test public void testStoredQuerySubquery2() throws Exception {
        helpTestOptimize("select x.e1 from (EXEC pm1.sq1()) as x WHERE x.e2 = 3",  //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "SELECT e1 FROM (EXEC pm1.sq1()) AS x WHERE e2 = 3"); //$NON-NLS-1$
    }

    @Test public void testOptimizeOrderBy() throws Exception {
        helpTestOptimize("SELECT pm1.g1.e1 FROM pm1.g1 order by pm1.g1.e1",  //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "SELECT e1 FROM pm1.g1 ORDER BY e1"); //$NON-NLS-1$
    }

    /**
     * It is by design that order by optimization only works in one direction.  It is not desirable to
     * fully qualify order by elements
     */
    @Test public void testOptimizeOrderBy1() throws Exception {
        helpTestFullyQualify("SELECT e1 FROM pm1.g1 order by e1",  //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "SELECT pm1.g1.e1 FROM pm1.g1 ORDER BY e1"); //$NON-NLS-1$
    }

    @Test public void testOptimizeOrderByWithoutGroup() throws Exception {
        helpTestOptimize("SELECT pm1.g1.e1, count(*) as x FROM pm1.g1 order by x",  //$NON-NLS-1$
                            RealMetadataFactory.example1Cached(),
                            "SELECT e1, COUNT(*) AS x FROM pm1.g1 ORDER BY x"); //$NON-NLS-1$
    }

    @Test public void testOutputNames() throws Exception {
        String sql = "select PM1.g1.e1, e2 FROM Pm1.G1";
        Command command = QueryParser.getQueryParser().parseCommand(sql);

        QueryMetadataInterface metadata = new BasicQueryMetadataWrapper(RealMetadataFactory.example1Cached()){
            public boolean useOutputName() {
                return false;
            };
        };
        QueryResolver.resolveCommand(command, metadata);

        assertEquals("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", command.toString());
    }

}
