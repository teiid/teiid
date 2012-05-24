package groovy.sql;

import groovy.lang.Closure;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collection;
import java.util.logging.Level;

import org.teiid.client.plan.PlanNode;
import org.teiid.jdbc.TeiidStatement;

/**
 * An extension of Groovy's Sql to support getting {@link TeiidStatement} specific properties.
 */
@SuppressWarnings("nls")
public final class TeiidSql extends Sql {
	private int maxRows;
	private PlanNode plan;
	private Collection<org.teiid.client.plan.Annotation> annotations;
	private String debugLog;
	private SQLWarning warnings;

	public TeiidSql(Connection connection) {
		super(connection);
	}
	
	@Override
	protected void configure(Statement statement) {
		super.configure(statement);
		try {
			statement.setMaxRows(maxRows);
		} catch (SQLException e) {
			LOG.log(Level.WARNING, "Caught exception setting max rows: " + e, e);
		}
		plan = null;
		annotations = null;
		debugLog = null;
		warnings = null;
	}
	
	/**
	 * Overridden to fix passing the resultset to the closure
	 */
	@Override
    public void eachRow(String sql, Closure metaClosure, Closure rowClosure) throws SQLException {
        AbstractQueryCommand command = createQueryCommand(sql);
        ResultSet results = null;
        try {
        	results = command.execute();
            if (metaClosure != null) metaClosure.call(results.getMetaData());

            GroovyResultSet groovyRS = new GroovyResultSetProxy(results).getImpl();
            while (groovyRS.next()) {
            	rowClosure.call(groovyRS);
            }
        } catch (SQLException e) {
            LOG.warning("Failed to execute: " + sql + " because: " + e.getMessage());
            throw e;
        } finally {
            command.closeResources();
        }
    }

	@Override
	protected void closeResources(Connection connection,
			Statement statement) {
		getPlanInfo(statement);
		super.closeResources(connection, statement);
	}

	@Override
	protected void closeResources(Connection connection,
			Statement statement, ResultSet results) {
		getPlanInfo(statement);
		super.closeResources(connection, statement, results);
	}

	protected void getPlanInfo(Statement s) {
		if (s == null) {
			return;
		}
		TeiidStatement ts;
		try {
			ts = s.unwrap(TeiidStatement.class);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		plan = ts.getPlanDescription();
		annotations = ts.getAnnotations();
		debugLog = ts.getDebugLog();
		try {
			warnings = ts.getWarnings();
		} catch (SQLException e) {
			LOG.log(Level.WARNING, "Caught exception getting warnings: " + e, e);
		}
	}
	
	public SQLWarning getSQLWarnings() {
		return warnings;
	}

	public int getMaxRows() {
		return maxRows;
	}

	public void setMaxRows(int maxRows) {
		this.maxRows = maxRows;
	}

	public PlanNode getPlan() {
		return plan;
	}

	public Collection<org.teiid.client.plan.Annotation> getAnnotations() {
		return annotations;
	}

	public String getDebugLog() {
		return debugLog;
	}
	
}