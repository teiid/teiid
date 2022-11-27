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

package org.teiid.query.optimizer;

import org.junit.Test;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestEqualityOnly {

    @Test public void testPushdown() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table t (x string options (searchable 'equality_only'))", "x", "y");
        //should push
        TestOptimizer.helpPlan("select x from t where x = 'a'", tm, new String[] {"SELECT g_0.x FROM y.t AS g_0 WHERE g_0.x = 'a'"});
        //should not push
        TestOptimizer.helpPlan("select x from t where x > 'b'", tm, new String[] {"SELECT g_0.x FROM y.t AS g_0"});
        TestOptimizer.helpPlan("select x from t where x like 'c%'", tm, new String[] {"SELECT g_0.x FROM y.t AS g_0"});
    }

}
