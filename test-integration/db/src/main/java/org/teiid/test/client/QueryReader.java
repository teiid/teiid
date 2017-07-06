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

import java.util.Collection;
import java.util.List;

import org.teiid.test.framework.exception.QueryTestFailedException;

/**
 * The QueryReader is responsible for providing a set queries for a given <code>querySetID</code>.
 * 
 * The querySetID identifies a set of queries to executed.
 * The Map structure is as follows:
 * 
 * Map: 	Key		-	Value
 * 		QueryIdentifier		SQL Query
 * 						
 * 
 * Where:
 * -	QueryIdentifier uniquely identifies a sql query in the set.
 * 	SQL Query is the query to be executed.
 * 
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public interface QueryReader {
    
    /**
     * {@link #PROP_QUERY_FILES_DIR_LOC} indicates the location to find the query files 
     */
    public static final String PROP_QUERY_FILES_DIR_LOC = "queryfiles.loc";
    /**
     * {@link #PROP_QUERY_FILES_ROOT_DIR}, if specified, indicates the root directory 
     * to be prepended to the {@link #PROP_QUERY_FILES_DIR_LOC} to create the full
     * directory to find the query files.  
     * 
     * This property is normally used during the nightly builds so that the query files
     * will coming from other projects.
     */
    public static final String PROP_QUERY_FILES_ROOT_DIR = "queryfiles.root.dir";

    
    /**
     * Return the <code>querySetID</code>s that identifies all the query sets
     * that are available to execute during this test.
     * 
     * @return
     */
    Collection<String> getQuerySetIDs();

    /**
     * Return a <code>List</code> containing {@link QueryTest}
     * 
     * @return List
     * @throws QueryTestFailedException
     * 
     * @since
     */
    List<QueryTest> getQueries(String querySetID)
	    throws QueryTestFailedException; 
																					// Map<String, String> - key = queryIdentifier

}
