/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.util;

import java.util.Properties;

import org.teiid.translator.ExecutionFactory.NullOrder;

/**
 * A holder for options
 */
public class Options {

    public static final String UNNEST_DEFAULT = "org.teiid.subqueryUnnestDefault"; //$NON-NLS-1$
    public static final String PUSHDOWN_DEFAULT_NULL_ORDER = "org.teiid.pushdownDefaultNullOrder"; //$NON-NLS-1$
    public static final String IMPLICIT_MULTISOURCE_JOIN = "org.teiid.implicitMultiSourceJoin"; //$NON-NLS-1$
    public static final String JOIN_PREFETCH_BATCHES = "org.teiid.joinPrefetchBatches"; //$NON-NLS-1$
    public static final String SANITIZE_MESSAGES = "org.teiid.sanitizeMessages"; //$NON-NLS-1$
    public static final String REQUIRE_COLLATION = "org.teiid.requireTeiidCollation"; //$NON-NLS-1$
    public static final String DEFAULT_NULL_ORDER = "org.teiid.defaultNullOrder"; //$NON-NLS-1$
    public static final String ASSUME_MATCHING_COLLATION = "org.teiid.assumeMatchingCollation"; //$NON-NLS-1$
    public static final String AGGRESSIVE_JOIN_GROUPING = "org.teiid.aggressiveJoinGrouping"; //$NON-NLS-1$
    public static final String MAX_SESSION_BUFFER_SIZE_ESTIMATE = "org.teiid.maxSessionBufferSizeEstimate"; //$NON-NLS-1$
    public static final String TRACING_WITH_ACTIVE_SPAN_ONLY = "org.teiid.tracingWithActiveSpanOnly"; //$NON-NLS-1$
    public static final String ENFORCE_SINGLE_MAX_BUFFER_SIZE_ESTIMATE = "org.teiid.enforceSingleMaxBufferSizeEstimate"; //$NON-NLS-1$

    private Properties properties;
    private boolean subqueryUnnestDefault = false;
    private boolean pushdownDefaultNullOrder;
    private boolean implicitMultiSourceJoin = true;
    private int joinPrefetchBatches = 10;
    private boolean sanitizeMessages;
    private boolean requireTeiidCollation;
    private NullOrder defaultNullOrder = NullOrder.LOW;
    private boolean assumeMatchingCollation = true;
    private boolean aggressiveJoinGrouping = true;
    private long maxSessionBufferSizeEstimate = Long.MAX_VALUE;
    private boolean tracingWithActiveSpanOnly = true;
    private boolean enforceSingleMaxBufferSizeEstimate = false;
    private boolean relativeXPath = true;

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public boolean isSubqueryUnnestDefault() {
        return subqueryUnnestDefault;
    }

    public void setSubqueryUnnestDefault(boolean subqueryUnnestDefault) {
        this.subqueryUnnestDefault = subqueryUnnestDefault;
    }

    public Options subqueryUnnestDefault(boolean s) {
        this.subqueryUnnestDefault = s;
        return this;
    }

    public boolean isPushdownDefaultNullOrder() {
        return pushdownDefaultNullOrder;
    }

    public void setPushdownDefaultNullOrder(boolean virtualizeDefaultNullOrdering) {
        this.pushdownDefaultNullOrder = virtualizeDefaultNullOrdering;
    }

    public Options pushdownDefaultNullOrder(boolean p) {
        this.pushdownDefaultNullOrder = p;
        return this;
    }

    public void setImplicitMultiSourceJoin(boolean implicitMultiSourceJoin) {
        this.implicitMultiSourceJoin = implicitMultiSourceJoin;
    }

    public boolean isImplicitMultiSourceJoin() {
        return implicitMultiSourceJoin;
    }

    public Options implicitMultiSourceJoin(boolean b) {
        this.implicitMultiSourceJoin = b;
        return this;
    }

    public void setJoinPrefetchBatches(int joinPrefetchBatches) {
        this.joinPrefetchBatches = joinPrefetchBatches;
    }

    public int getJoinPrefetchBatches() {
        return joinPrefetchBatches;
    }

    public Options joinPrefetchBatches(int i) {
        this.joinPrefetchBatches = i;
        return this;
    }

    public void setSanitizeMessages(boolean sanitizeMessages) {
        this.sanitizeMessages = sanitizeMessages;
    }

    public boolean isSanitizeMessages() {
        return sanitizeMessages;
    }

    public Options sanitizeMessages(boolean b) {
        this.sanitizeMessages = b;
        return this;
    }

    public boolean isRequireTeiidCollation() {
        return requireTeiidCollation;
    }

    public void setRequireTeiidCollation(boolean requireTeiidCollation) {
        this.requireTeiidCollation = requireTeiidCollation;
    }

    public Options requireTeiidCollation(boolean b) {
        this.requireTeiidCollation = b;
        return this;
    }

    public NullOrder getDefaultNullOrder() {
        return defaultNullOrder;
    }

    public void setDefaultNullOrder(NullOrder defaultNullOrder) {
        this.defaultNullOrder = defaultNullOrder;
    }

    public Options defaultNullOrder(NullOrder b) {
        this.defaultNullOrder = b;
        return this;
    }

    public boolean isAssumeMatchingCollation() {
        return this.assumeMatchingCollation;
    }

    public void setAssumeMatchingCollation(boolean assumeMatchingCollation) {
        this.assumeMatchingCollation = assumeMatchingCollation;
    }

    public Options assumeMatchingCollation(boolean b) {
        this.assumeMatchingCollation = b;
        return this;
    }

    public boolean isAggressiveJoinGrouping() {
        return this.aggressiveJoinGrouping;
    }

    public void setAggressiveJoinGrouping(boolean aggressiveJoinGrouping) {
        this.aggressiveJoinGrouping = aggressiveJoinGrouping;
    }

    public Options aggressiveJoinGrouping(boolean b) {
        this.aggressiveJoinGrouping = b;
        return this;
    }

    public Options maxSessionBufferSizeEstimate(
            long l) {
        this.maxSessionBufferSizeEstimate = l;
        return this;
    }

    public void setMaxSessionBufferSizeEstimate(
            long maxSessionBufferSizeEstimate) {
        this.maxSessionBufferSizeEstimate = maxSessionBufferSizeEstimate;
    }

    public long getMaxSessionBufferSizeEstimate() {
        return maxSessionBufferSizeEstimate;
    }

    public void setTracingWithActiveSpanOnly(
            boolean tracingWithActiveSpanOnly) {
        this.tracingWithActiveSpanOnly = tracingWithActiveSpanOnly;
    }

    public boolean isTracingWithActiveSpanOnly() {
        return tracingWithActiveSpanOnly;
    }

    public Options tracingWithActiveSpanOnly(boolean b) {
        this.tracingWithActiveSpanOnly = b;
        return this;
    }

    public boolean isEnforceSingleMaxBufferSizeEstimate() {
        return enforceSingleMaxBufferSizeEstimate;
    }

    public void setEnforceSingleMaxBufferSizeEstimate(
            boolean enforceSingleMaxBufferSizeEstimate) {
        this.enforceSingleMaxBufferSizeEstimate = enforceSingleMaxBufferSizeEstimate;
    }

    public Options enforceSingleMaxBufferSizeEstimate(
            boolean b) {
        this.enforceSingleMaxBufferSizeEstimate = b;
        return this;
    }

    public boolean isRelativeXPath() {
        return relativeXPath;
    }

    public void setRelativeXPath(boolean relativeXPath) {
        this.relativeXPath = relativeXPath;
    }

    public Options relativeXPath(
            boolean b) {
        this.relativeXPath = b;
        return this;
    }

}
