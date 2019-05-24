/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.teiid.cdk.unittest;

import java.util.List;

import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.Column.SearchType;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class FakeTranslationFactory {

    private static FakeTranslationFactory instance = new FakeTranslationFactory();

    public static FakeTranslationFactory getInstance() {
        return instance;
    }

    public TranslationUtility getBQTTranslationUtility() {
        return new TranslationUtility(RealMetadataFactory.exampleBQTCached());
    }

    public TranslationUtility getYahooTranslationUtility() {
        MetadataStore metadataStore = new MetadataStore();
        // Create models
        Schema yahoo = RealMetadataFactory.createPhysicalModel("Yahoo", metadataStore); //$NON-NLS-1$

        // Create physical groups
        Table quotes = RealMetadataFactory.createPhysicalGroup("Yahoo.QuoteServer", yahoo); //$NON-NLS-1$

        // Create physical elements
        String[] elemNames = new String[] {
            "TickerSymbol", "LastTrade",  //$NON-NLS-1$ //$NON-NLS-2$
            "LastTradeDate", "LastTradeTime", //$NON-NLS-1$ //$NON-NLS-2$
            "PercentageChange", "TickerSymbol2",  //$NON-NLS-1$ //$NON-NLS-2$
            "DaysHigh", "DaysLow",  //$NON-NLS-1$ //$NON-NLS-2$
            "TotalVolume"             //$NON-NLS-1$
        };
        String[] elemTypes = new String[] {
            DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DOUBLE,
            DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIME,
            DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING,
            DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE,
            DataTypeManager.DefaultDataTypes.BIG_INTEGER
        };

        List<Column> cols = RealMetadataFactory.createElements(quotes, elemNames, elemTypes);

        // Set name in source on each column
        String[] nameInSource = new String[] {
           "Symbol", "Last", "Date", "Time", "Change", "Symbol2", "High", "Low", "Volume"
        };
        for(int i=0; i<nameInSource.length; i++) {
            cols.get(i).setNameInSource(nameInSource[i]);
        }

        // Set column-specific properties
        cols.get(0).setSelectable(false);
        cols.get(0).setSearchType(SearchType.Unsearchable);

        return new TranslationUtility(RealMetadataFactory.createTransformationMetadata(metadataStore, "yahoo"));
    }

    public TranslationUtility getExampleTranslationUtility() {
        return new TranslationUtility(RealMetadataFactory.example1Cached());
    }

}
