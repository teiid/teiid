/*
 * ${license}
 */
package ${package-name};

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.basic.BasicConnectorCapabilities;


/**
 * Specifies the capabilities of this connector.
 */
public class ${connector-name}Capabilities extends BasicConnectorCapabilities {

	// TODO: override "supports*" methods from the BaseConnectorCapabilities to
	// extend the capabilities of your connector. By specifying the correct capabilities
	// Teiid will push certain commands and joins etc to be handled by the connector,
	// which is more performent.
}
