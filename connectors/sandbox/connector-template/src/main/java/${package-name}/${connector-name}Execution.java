/*
 * ${license}
 */
package ${package-name};

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.IBatchedUpdates;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IDelete;
import org.teiid.connector.language.IInsert;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.language.IUpdate;
import org.teiid.connector.language.IParameter.Direction;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;


/**
 * Execution of a command. This may be select, update or procedure command. 
 */
public class ${connector-name}Execution extends BasicExecution implements UpdateExecution, ProcedureExecution {

    // Connector resources
    private ${connector-name}ManagedConnectionFactory config;
    private ICommand command;
    private RuntimeMetadata metadata;
    
    private List<Object> row;
    
    public ${connector-name}Execution(ICommand command, ${connector-name}ManagedConnectionFactory config, RuntimeMetadata metadata) {
        this.config = config;
        this.command = command;
        this.metadata = metadata;
    }
    
    @Override
    public void execute() throws ConnectorException {
       
    	// Log our command
        this.config.getLogger().logTrace("executing command: " + command); //$NON-NLS-1$

        // Note that a connector does not have support all types of commands always. If you are
        // writing read-only then "query" may be sufficient.
        if (command instanceof IQueryCommand) {
            IQueryCommand queryCommand = (IQueryCommand)command;
            // TODO: execute and produce results for "select" command
        }
        else if (command instanceof IInsert) {
        	// TODO: fill in for "insert" command support
        }
        else if (command instanceof IUpdate) {
        	// TODO: fill in for "update" command support
        }
        else if (command instanceof IDelete) {
        	// TODO: fill in for "delete" command support
        }
        else if (command instanceof IProcedure) {
        	// TODO: fill in for "procedure" command support
        }
        else if (command instanceof IBatchedUpdates) {
        	// TODO: fill in for "batched updates" command support
        }
        
        // Note here that we executed the command, however the results are read by calling
        // next() or getUpdateCounts() or getOutputParameterValues() methods.
        List types = determineOutputTypes(this.command);
        createDummyRow(types);        
    }
    
    @Override
    public List<?> next() throws ConnectorException, DataNotAvailableException {
    	// create and return one row at a time for your resultset.
        
        return row;
    }
    
    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, ConnectorException {
    	return new int [] {0};
    }
    
    @Override
    public List<?> getOutputParameterValues() throws ConnectorException {
    	IProcedure proc = (IProcedure)this.command;
    	int count = 0;
    	for (IParameter param : proc.getParameters()) {
			if (param.getDirection() == Direction.INOUT || param.getDirection() == Direction.OUT || param.getDirection() == Direction.RETURN) {
				count++;
			}
		}
    	return Arrays.asList(new Object[count]);
    }


    @Override
    public void close() throws ConnectorException {
        // TODO:cleanup your execution based resources here
    }

    @Override
    public void cancel() throws ConnectorException {
    	//TODO: initiate the "abort" of execution 
    }

    private List determineOutputTypes(ICommand command) throws ConnectorException {            
        // Get select columns and lookup the types in metadata
        if(command instanceof IQueryCommand) {
            IQueryCommand query = (IQueryCommand) command;
            return Arrays.asList(query.getColumnTypes());
        }
       
        if (command instanceof IProcedure) {
        	return Arrays.asList(((IProcedure)command).getResultSetColumnTypes());
        }

        // this is for insert/update/delete calls
        List<Class<?>> types = new ArrayList<Class<?>>(1);
        types.add(Integer.class);
        return types;
    }
    
    // TODO: replace this method with your own.
    private void createDummyRow(List<Class<?>> types) {
        row = new ArrayList<Object>(types.size());
        
        for (Class<?> type : types) {
            row.add(getValue(type) );
        }   
    }
    
    // TODO: this method is provided for example purposes only for creating a dummy row.
    private Object getValue(Class type) {
    	Calendar cal = Calendar.getInstance();
    	cal.clear();
    	cal.set(1969, 11, 31, 18, 0, 0);
    	
        if(type.equals(java.lang.String.class)) {
            return "some string value";
        } else if(type.equals(java.lang.Integer.class)) {
            return new Integer(0);
        } else if(type.equals(java.lang.Short.class)) { 
            return new Short((short)0);    
        } else if(type.equals(java.lang.Long.class)) {
            return new Long(0);
        } else if(type.equals(java.lang.Float.class)) {
            return new Float(0.0);
        } else if(type.equals(java.lang.Double.class)) {
            return new Double(0.0);
        } else if(type.equals(java.lang.Character.class)) {
            return new Character('c');
        } else if(type.equals(java.lang.Byte.class)) {
            return new Byte((byte)0);
        } else if(type.equals(java.lang.Boolean.class)) {
            return Boolean.FALSE;
        } else if(type.equals(java.math.BigInteger.class)) {
            return new BigInteger("0");
        } else if(type.equals(java.math.BigDecimal.class)) {
            return new BigDecimal("0");
        } else if(type.equals(java.sql.Date.class)) {
            return new Date(cal.getTimeInMillis());
        } else if(type.equals(java.sql.Time.class)) {
            return new Time(cal.getTimeInMillis());
        } else if(type.equals(java.sql.Timestamp.class)) {
            return new Timestamp(cal.getTimeInMillis());
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.CLOB)) {
            return this.config.getTypeFacility().convertToRuntimeType("some string value");
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BLOB)) {
            return this.config.getTypeFacility().convertToRuntimeType("some string value");
        } else {
            return "some string value";
        }
    }    

}
