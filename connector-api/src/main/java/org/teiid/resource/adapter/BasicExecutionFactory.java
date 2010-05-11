package org.teiid.resource.adapter;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Properties;

import org.teiid.connector.language.Call;
import org.teiid.connector.language.Command;
import org.teiid.connector.language.LanguageFactory;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.cci.ConnectorCapabilities;
import org.teiid.resource.cci.Execution;
import org.teiid.resource.cci.ExecutionContext;
import org.teiid.resource.cci.ExecutionFactory;
import org.teiid.resource.cci.ProcedureExecution;
import org.teiid.resource.cci.ResultSetExecution;
import org.teiid.resource.cci.TypeFacility;
import org.teiid.resource.cci.UpdateExecution;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.core.util.StringUtil;

public class BasicExecutionFactory implements ExecutionFactory {

	protected ConnectorCapabilities capabilities;
	private static final TypeFacility TYPE_FACILITY = new TypeFacility();
	
	private String capabilitiesClass;
	private boolean immutable = false;
	private boolean exceptionOnMaxRows = false;
	private int maxResultRows = -1;
	private boolean xaCapable;
	private String overrideCapabilitiesFile;
	private boolean sourceRequired = true;

	@Override
	public void start() throws ConnectorException {
	}	
	
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return BasicConnectorCapabilities.class;
    }	
    
    @Override
    public ConnectorCapabilities getCapabilities() throws ConnectorException {
    	if (capabilities == null) {
			// create Capabilities
    		capabilities = BasicManagedConnectionFactory.getInstance(ConnectorCapabilities.class, getCapabilitiesClass(), null, getDefaultCapabilities());
    	}
    	
		// capabilities overload
    	Properties props = getOverrideCapabilities();
		if (this.capabilities != null && props != null) {
			this.capabilities = (ConnectorCapabilities) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {ConnectorCapabilities.class}, new CapabilitesOverloader(this.capabilities, props));
		}
    	return capabilities;
	}    
    
	@Override
	public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory) throws ConnectorException {
		if (command instanceof QueryExpression) {
			return createResultSetExecution((QueryExpression)command, executionContext, metadata, connectionFactory);
		}
		if (command instanceof Call) {
			return createProcedureExecution((Call)command, executionContext, metadata, connectionFactory);
		}
		return createUpdateExecution(command, executionContext, metadata, connectionFactory);
	}

	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution"); //$NON-NLS-1$
	}

	public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution");//$NON-NLS-1$
	}

	public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution");//$NON-NLS-1$
	}    
	
	// derived
	private Properties overrideCapabilities;
	
	
	@Override
	public LanguageFactory getLanguageFactory() {
		return LanguageFactory.INSTANCE;
	}

	@Override
	public TypeFacility getTypeFacility() {
		return TYPE_FACILITY;
	}

	@Override
	public Properties getOverrideCapabilities() throws ConnectorException {
		if (this.overrideCapabilities == null && getOverrideCapabilitiesFile() != null) {
			try {
				this.overrideCapabilities = new Properties();
				this.overrideCapabilities.loadFromXML(this.getClass().getResourceAsStream(getOverrideCapabilitiesFile()));
			} catch (IOException e) {
				throw new ConnectorException(e);
			}
		}
		return this.overrideCapabilities;
	}

    public static <T> T getInstance(Class<T> expectedType, String className, Collection ctorObjs, Class defaultClass) throws ConnectorException {
    	try {
	    	if (className == null) {
	    		if (defaultClass == null) {
	    			throw new ConnectorException("Neither class name or default class specified to create an instance"); //$NON-NLS-1$
	    		}
	    		return expectedType.cast(defaultClass.newInstance());
	    	}
	    	return expectedType.cast(ReflectionHelper.create(className, ctorObjs, Thread.currentThread().getContextClassLoader()));
		} catch (MetaMatrixCoreException e) {
			throw new ConnectorException(e);
		} catch (IllegalAccessException e) {
			throw new ConnectorException(e);
		} catch(InstantiationException e) {
			throw new ConnectorException(e);
		}    	
    }	
	/**
	 * Overloads the connector capabilities with one defined in the connector binding properties
	 */
    static final class CapabilitesOverloader implements InvocationHandler {
    	ConnectorCapabilities caps; 
    	Properties properties;
    	
    	CapabilitesOverloader(ConnectorCapabilities caps, Properties properties){
    		this.caps = caps;
    		this.properties = properties;
    	}
    	
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String value = this.properties.getProperty(method.getName());
			if (value == null || value.trim().length() == 0 || (args != null && args.length != 0)) {
				return method.invoke(this.caps, args);
			}
			return StringUtil.valueOf(value, method.getReturnType());
		}
	}
    
    @Override
	public String getCapabilitiesClass() {
		return capabilitiesClass;
	}

	public void setCapabilitiesClass(String arg0) {
		this.capabilitiesClass = arg0;
	}

	@Override
	public boolean isImmutable() {
		return immutable;
	}
	
	public void setImmutable(boolean arg0) {
		this.immutable = arg0;
	}	

	@Override
	public boolean isExceptionOnMaxRows() {
		return exceptionOnMaxRows;
	}
	
	public void setExceptionOnMaxRows(boolean arg0) {
		this.exceptionOnMaxRows = arg0;
	}

	@Override
	public int getMaxResultRows() {
		return maxResultRows;
	}

	public void setMaxResultRows(int arg0) {
		this.maxResultRows = arg0;
	}

	@Override
	public boolean isXaCapable() {
		return xaCapable;
	}

	public void setXaCapable(boolean arg0) {
		this.xaCapable = arg0;
	}

	@Override
	public String getOverrideCapabilitiesFile() throws ConnectorException {
		return this.overrideCapabilitiesFile;
	}
	
	
	public void setOverrideCapabilitiesFile(String overrideCapabilitiesFile) {
		this.overrideCapabilitiesFile = overrideCapabilitiesFile;
	}

	@Override
	public boolean isSourceRequired() {
		return sourceRequired;
	}	
	
	public void setSourceRequired(boolean value) {
		this.sourceRequired = value;
	}
}
