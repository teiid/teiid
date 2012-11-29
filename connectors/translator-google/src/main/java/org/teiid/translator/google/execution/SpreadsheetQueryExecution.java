package org.teiid.translator.google.execution;

import java.util.Iterator;
import java.util.List;

import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.google.GoogleSpreadsheetConnection;
import org.teiid.resource.adapter.google.common.SheetRow;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.google.SpreadsheetExecutionFactory;

public class SpreadsheetQueryExecution implements ResultSetExecution {

	private Select query;
	private GoogleSpreadsheetConnection connection;
	private Iterator<SheetRow> rowIterator;
	private ExecutionContext executionContext;

	public SpreadsheetQueryExecution(Select query,
			GoogleSpreadsheetConnection connection, ExecutionContext executionContext) {
		this.executionContext = executionContext;
		this.connection = connection;
		this.query = query;
	}

	@Override
	public void close() {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("close_query"));
	}

	@Override
	public void cancel() throws TranslatorException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("cancel_query"));

	}

	@Override
	public void execute() throws TranslatorException {
		SpreadsheetSQLVisitor visitor = new SpreadsheetSQLVisitor();
		visitor.translateSQL(query);		
		rowIterator = connection.executeQuery(visitor.getWorksheetTitle(), visitor.getTranslatedSQL(), visitor.getOffsetValue(),visitor.getLimitValue(), executionContext.getBatchSize()).iterator();
		
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {		
		if (rowIterator.hasNext()) {
			return rowIterator.next().getRow();
		}
		return null;
	}

}
