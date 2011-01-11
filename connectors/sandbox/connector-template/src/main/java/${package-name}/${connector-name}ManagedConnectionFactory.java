/*
 * ${license}
 */
package ${package-name};

import org.teiid.connector.basic.BasicManagedConnectionFactory;

public class ${connector-name}ManagedConnectionFactory extends BasicManagedConnectionFactory {
	
	@Override
	public BasicConnectionFactory createConnectionFactory() throws ResourceException {
		return new BasicConnectionFactory() {
			@Override
			public BasicConnection getConnection() throws ResourceException {
				return new ${connector-name}ConnectionImpl(this);
			}
		};
	}	
	
	// ra.xml files getters and setters go here.

}
