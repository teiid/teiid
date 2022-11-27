package org.teiid.translator.loopback;

import java.util.List;

import org.junit.Assert;
import org.teiid.cdk.api.ConnectorHost;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.translator.TranslatorException;

public class TestHelper {
    public static LoopbackExecutionFactory exampleProperties(boolean incrementRows,int waitTime, int rowCount) {
        LoopbackExecutionFactory config = new LoopbackExecutionFactory();
        config.setWaitTime(waitTime);
        config.setRowCount(rowCount);
        config.setIncrementRows(incrementRows);
        return config;
    }

    public static void helpTestQuery(boolean incrementRows, String sql, TranslationUtility metadata, int waitTime, int rowCount, Object[][] expectedResults) throws TranslatorException {
        ConnectorHost host = new ConnectorHost(exampleProperties(incrementRows,waitTime, rowCount), null, metadata);

        List actualResults = host.executeCommand(sql);

        // Compare actual and expected results
        Assert.assertEquals("Did not get expected number of rows", expectedResults.length, actualResults.size()); //$NON-NLS-1$

        if(expectedResults.length > 0) {
            // Compare column sizes
            Assert.assertEquals("Did not get expected number of columns", expectedResults[0].length, ((List)actualResults.get(0)).size()); //$NON-NLS-1$

            // Compare results
            for(int r=0; r<expectedResults.length; r++) {
                Object[] expectedRow = expectedResults[r];
                List actualRow = (List) actualResults.get(r);

                for(int c=0; c<expectedRow.length; c++) {
                    Object expectedValue = expectedRow[c];
                    Object actualValue = actualRow.get(c);

                    if(expectedValue == null) {
                        if(actualValue != null) {
                            Assert.fail("Row " + r + ", Col " + c + ": Expected null but got " + actualValue + " of type " + actualValue.getClass().getName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                        }
                    } else if(actualValue == null) {
                        Assert.fail("Row " + r + ", Col " + c + ": Expected " + expectedValue + " but got null"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                    } else if (expectedValue.getClass().isArray() && !expectedValue.getClass().getComponentType().isPrimitive()) {
                        Assert.assertArrayEquals("Row " + r + ", Col " + c + ": Expected " + expectedValue + " but got " + actualValue, (Object[])expectedValue, (Object[])actualValue); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                    } else {
                        Assert.assertEquals("Row " + r + ", Col " + c + ": Expected " + expectedValue + " but got " + actualValue, expectedValue, actualValue); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                    }
                }
            }
        }
    }
}
