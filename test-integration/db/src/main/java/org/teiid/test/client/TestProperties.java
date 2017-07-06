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
package org.teiid.test.client;

/**
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class TestProperties {
    
    /**
     * PROP_SCENARIO_FILE indicates the scenario properties file to load.
     */
    public static final String PROP_SCENARIO_FILE = "scenariofile";
    
    /**
     * The {@link #QUERY_SET_NAME} property indicates the name of directory that contains
     * the set of queries and expected results that will be used.  This is referred
     * to as the <b>query set</b> 
     * 
     * This property should be found in the {@link #PROP_SCENARIO_FILE}. 
     */
    public static final String QUERY_SET_NAME = "queryset.dir"; //$NON-NLS-1$
    
    /**
     * PROP_RESULT_MODE controls what to do with the execution results.
     * 
     * @see ExpectedResults.RESULT_MODES for the options.
     */
    public static final String PROP_RESULT_MODE = "resultmode"; 

    /**
     * All test options will produce the following basic information at the end
     * of the test process: <li>how many queries were run</li> <li>how many were
     * successfull</li> <li>how many errored</li> <li>the execution time for
     * each query</li> <li>total time for all the tests to run</li>
     */
    
   public interface RESULT_MODES {
        /**
         * NONE - will provide the basic information
         */
        static final String NONE = "none";
        /**
         * COMPARE - will provide the following information, in addition to the
         * basic information <li>compare actual results with expected results
         * and produce error files where expected results were not accomplished</li>
         */
        static final String COMPARE = "compare";
        /**
         * GENERATE - will provide the following information, in addition to the
         * basic information <li>will generate a new set of expected results
         * files to the defined PROP_GENERATAE_DIR directory.
         */
        static final String GENERATE = "generate";
    }

    /**
     * The {@link #PROP_OUTPUT_DIR} property indicates the root directory that output
     * files will be written.  
     */
    public static final String PROP_OUTPUT_DIR = "outputdir"; //$NON-NLS-1$
    
    



}
