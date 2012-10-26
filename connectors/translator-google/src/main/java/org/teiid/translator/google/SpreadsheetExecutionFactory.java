package org.teiid.translator.google;

import javax.resource.cci.ConnectionFactory;

import org.teiid.core.BundleUtil;
import org.teiid.language.Call;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adapter.google.GoogleSpreadsheetConnection;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.google.execution.SpreadsheetProcedureExecution;
import org.teiid.translator.google.execution.SpreadsheetQueryExecution;
import org.teiid.translator.google.metadata.MetadataProcessor;




@Translator(name="google-spreadsheet", description="A translator for Google Spreadsheet")
public class SpreadsheetExecutionFactory extends ExecutionFactory<ConnectionFactory, GoogleSpreadsheetConnection>{
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(SpreadsheetExecutionFactory.class);
	public SpreadsheetExecutionFactory() {
		setSourceRequiredForMetadata(false);
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Google Spreadsheet ExecutionFactory Started"); //$NON-NLS-1$
	}


	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, GoogleSpreadsheetConnection connection)
			throws TranslatorException {
		return new SpreadsheetQueryExecution((Select)command, connection);
	}
	
	@Override
	public ProcedureExecution createProcedureExecution(Call command,ExecutionContext executionContext, RuntimeMetadata metadata, GoogleSpreadsheetConnection connection)
			throws TranslatorException {
		return new SpreadsheetProcedureExecution(command, connection);
	}

	@Override
	public void getMetadata(MetadataFactory metadataFactory, GoogleSpreadsheetConnection connection) throws TranslatorException {
		MetadataProcessor metadataProcessor=new MetadataProcessor(metadataFactory, connection.getSpreadsheetInfo());
	 	metadataProcessor.processMetadata();
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
	    public boolean supportsLikeCriteria() {
	        return true;
	    }
	    
	    @Override
	    public boolean supportsOrCriteria() {
	    	return true;
	    }
	    
	    @Override
	    public boolean supportsNotCriteria() {
	    	return true;
	    }
	    
	    @Override
	    public boolean supportsAggregatesCount() {
	    	return true;
	    }
	    
	    @Override
	    public boolean supportsAggregatesMax() {
	    	return true;
	    }

	    @Override
	    public boolean supportsAggregatesMin() {
	    	return true;
	    }

	    @Override
	    public boolean supportsAggregatesSum() {
	    	return true;
	    }
	    
	    @Override
	    public boolean supportsAggregatesAvg() {
	    	return true;
	    }
	    
	    @Override
	    public boolean supportsGroupBy() {
	    	return true;
	    }
	    
	    @Override
	    public boolean supportsOrderBy() {
	    	return true;
	    }
	    
	    @Override
	    public boolean supportsHaving() {
	    	return false;
	    }
	    @Override
	    public boolean supportsCompareCriteriaOrdered() {
	    	return true;
	    }
	    @Override
	    public boolean supportsRowLimit() {
	    	return true;
	    }
	    @Override
	    public boolean supportsRowOffset() {
	    	return true;
	    }
	
}
