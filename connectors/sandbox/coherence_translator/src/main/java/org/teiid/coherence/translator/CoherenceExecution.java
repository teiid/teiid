/*
 * ${license}
 */
package org.teiid.coherence.translator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.resource.ResourceException;

import org.teiid.coherence.connector.CoherenceConnection;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;



/**
 * Execution of a command. This may be select, update or procedure command. 
 */
public class CoherenceExecution implements ResultSetExecution {

    // Connector resources
    private CoherenceExecutionFactory config;
    private RuntimeMetadata metadata;
    private Select query;
    private List results;
    private CoherenceConnection connection;
    
    private Iterator resultsIt = null;
    
    private List<Object> row;
    
    public CoherenceExecution(Select query, RuntimeMetadata metadata, CoherenceConnection connection) {
        this.metadata = metadata;
        this.query = query;
        this.connection = connection;
    }
    
    @Override
    public void execute() throws TranslatorException {
        // Log our command
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Coherence executing command: " + query); //$NON-NLS-1$

        // Build url
        Map<String, List<Long>> targetArguments = translateQuery(query);
        
        // Execute url to get results
        this.results = executeQuery(targetArguments);
        this.resultsIt = this.results.iterator();
    }     
    
    private Map<String, List<Long>> translateQuery(Select query) throws TranslatorException {
    	
    	CoherenceVisitor visitor = new CoherenceVisitor(metadata);
    	visitor.visitNode(query);
    	
        if(visitor.getException() != null) { 
            throw visitor.getException();
        }
        return visitor.getCriteria();

    }

	protected List executeQuery(Map<String, List<Long>> arguments)
			throws TranslatorException {
		List rows = new ArrayList();

		String tablename = (String) arguments.keySet().iterator().next();
		List<Long> args = arguments.get(tablename);

		try {
			String parm = null;
			for (Iterator<Long> it = args.iterator(); it.hasNext();) {
				Long t = it.next();
				if (parm != null) {
					parm += ",";
				}
				parm = String.valueOf(t + "l");

			}
			
			List<Object> trades = this.connection.get("Id in (" + parm + ")");
			if (trades == null)
				return rows;

			List<Object> row = new ArrayList<Object>(trades.size());

			for (Iterator<Object> it = trades.iterator(); it.hasNext();) {
				Trade t = (Trade) it.next();

				Map legsMap = t.getLegs();

				Set legsSet = legsMap.entrySet();
				Iterator k = legsSet.iterator();
				while (k.hasNext()) {
					Map.Entry legsEntry = (Map.Entry) k.next();
					Leg leg = (Leg) legsEntry.getValue();
					
					row = new ArrayList<Object>(3);
					
					// the types loaded here must match @link{CoherenceExecutionFactory#getMetadata)
					row.add(new Long(t.getId()));
					row.add(new Long(leg.getId()));
					row.add(new Double(leg.getNotional()));
					
					rows.add(row);

				}
			}

			return rows;

		} catch (ResourceException re) {
			re.printStackTrace();
		}

		return rows;
	}
    
    
    
    
    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
    	// create and return one row at a time for your resultset.
    	if (resultsIt.hasNext()) {
    		return (List) resultsIt.next();
    	}
        
        return null;
    }
    

    @Override
    public void close() {
        // TODO:cleanup your execution based resources here
    }

    @Override
    public void cancel() throws TranslatorException {
    	//TODO: initiate the "abort" of execution 
    }
  

}
