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

package org.teiid.query.validator;

import org.junit.Test;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestAlterValidation {

    @Test public void testValidateAlterView() {
        TestValidator.helpValidate("alter view SmallA_2589 as select 2", new String[] {"SELECT 2"}, RealMetadataFactory.exampleBQTCached());
        TestValidator.helpValidate("alter view Defect15355 as select 'a', 1", new String[] {"SELECT 'a', 1"}, RealMetadataFactory.exampleBQTCached());
        TestValidator.helpValidate("alter view Defect15355 as select 'a', cast(1 as biginteger)", new String[] {}, RealMetadataFactory.exampleBQTCached());

        TestValidator.helpValidate("alter view SmallA_2589 as select * from bqt1.smalla", new String[] {}, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testValidateAlterTrigger() {
        TestValidator.helpValidate("alter trigger on SmallA_2589 instead of insert as for each row begin atomic select 1; end", new String[] {"SmallA_2589"}, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testValidateAlterProcedure() {
        TestValidator.helpValidate("alter procedure spTest8a as begin select 1; end", new String[] {"spTest8a"}, RealMetadataFactory.exampleBQTCached());
        TestValidator.helpValidate("alter procedure MMSP1 as begin select 1; end", new String[] {"SELECT 1;"}, RealMetadataFactory.exampleBQTCached());
    }

}
