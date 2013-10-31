package org.teiid.translator.jdbc;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface ValueRetriever {
	
	/**
	 * Retrieve the value on the current resultset row for the given column index.
	 * @param results
	 * @param columnIndex
	 * @param expectedType
	 * @return the value
	 * @throws SQLException
	 */
    Object retrieveValue(ResultSet results, int columnIndex, Class<?> expectedType) throws SQLException;

    	
    /**
     * Retrieve the value for the given parameter index
     * @param results
     * @param parameterIndex
     * @param expectedType
     * @return the value
     * @throws SQLException
     */
    Object retrieveValue(CallableStatement results, int parameterIndex, Class<?> expectedType) throws SQLException;

}
