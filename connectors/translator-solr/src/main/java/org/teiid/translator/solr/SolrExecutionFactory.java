package org.teiid.translator.solr;

import java.math.BigDecimal;
import java.math.BigInteger;

import javax.resource.cci.ConnectionFactory;

import org.teiid.translator.solr.execution.SolrQueryExecution;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;

/**
 * Creates a execution factory
 * 
 * @author Jason Marley
 * TODO add configuration criteria to resource adaptor to enable orderBy, sort and limit 
 */
@Translator(name = "solr", description = "A translator for Solr search platform")
public class SolrExecutionFactory extends
		ExecutionFactory<ConnectionFactory, SolrConnection> {

	@Override
	public void start() throws TranslatorException {
		super.start();

		// check support order by
		// if (condition) {
		// this.setSupportsOrderBy(true);
		// }
		//
		// // check support for sort mechanism
		// if (condition) {
		// this.setSupportsOrderBy(true);
		// }

	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			SolrConnection connection) throws TranslatorException {
		return new SolrQueryExecution(command, executionContext, metadata,
				connection);
	}

	/**
	 * Description: casts the column value returned by Solr to what is expected
	 * my the Teiid table
	 * 
	 * @param columnValue
	 * @param columnType
	 * @return
	 * 
	 */
	public Object convertToTeiid(Object columnValue, Class<?> columnType) {
		if (columnValue == null) {
			return null;
		}

		try {
			if (columnType.equals(java.sql.Date.class)) {
				return new java.sql.Date(
						((java.util.Date) columnValue).getTime());
			} else if (columnType.equals(java.sql.Timestamp.class)) {
				return new java.sql.Timestamp(
						((java.util.Date) columnValue).getTime());
			} else if (columnType.equals(java.sql.Time.class)) {
				return new java.sql.Time(
						((java.util.Date) columnValue).getTime());
			} else if (columnType.equals(String.class)) {
				return new String((String) columnValue);
			} else if (columnType.equals(Integer.class)) {
				return new Integer((Integer) columnValue);
			} else if (columnType.equals(BigDecimal.class)) {
				return new BigDecimal(columnValue.toString());
			} else if (columnType.equals(BigInteger.class)) {
				return new BigInteger(columnValue.toString());
			} else if (columnType.equals(Character.class)) {
				return new Character(((Character) columnValue).charValue());
			} else if (columnType.equals(Double.class)) {
				return new Double(columnValue.toString()); 
			} else if (columnType.equals(Boolean.class)) {
				return new Boolean(columnValue.toString()); 
			} else if (columnType.equals(Long.class)) {
				return new Long(columnValue.toString()); 
			} else {
				LogManager
						.logWarning(
								columnType.toString(),
								"This '"
										+ columnType.toString()
										+ "' column type is not supported. Attempting to cast as string.");

				return new String((String) columnValue);
			}
		} catch (ClassCastException e) {
			throw new ClassCastException(
					"Could not cast field class type, check model and verify support. Field Value: "
							+ columnValue + " and Field Type: " + columnType);
		}

	}

	@Override
	public boolean supportsCompareCriteriaEquals() {
		return true;
	}

	@Override
	public boolean supportsInCriteria() {
		return true;
	}

	@Override
	public boolean supportsRowLimit() {
		return true;
	}

	@Override
	public boolean supportsNotCriteria() {
		return true;
	}

	// @Override
	// public void setSupportsOrderBy(boolean supportsOrderBy) {
	// super.setSupportsOrderBy(supportsOrderBy);
	// }

	@Override
	public boolean supportsLikeCriteria() {
		return true;
	}

	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return true;
	}

}
