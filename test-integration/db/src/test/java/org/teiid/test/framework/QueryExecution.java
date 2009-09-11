package org.teiid.test.framework;

import java.sql.Connection;

import com.metamatrix.jdbc.api.AbstractQueryTest;

/**
 * The QueryExecution class can be used to query the source directly.   The intended use
 * of this class is for validating results after the execution of a test case.
 * 
 * @see AbstractQueryTransactionTest regarding creating a testcase to validate behavior.
 * @author vanhalbert
 *
 */
public class QueryExecution extends AbstractQueryTest {
	
	public QueryExecution(Connection conn) {
		super(conn);
	}

}
