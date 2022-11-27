package org.teiid.translator.jdbc.sybase;

import org.teiid.translator.Translator;
import org.teiid.translator.jdbc.sap.SAPIQExecutionFactory;

/**
 * A translator for Sybase IQ 15.1+
 */
@Translator(name="sybaseiq", description="A translator for Sybase Database", deprecated="sap-iq")
public class SybaseIQExecutionFactory extends SAPIQExecutionFactory {

}
