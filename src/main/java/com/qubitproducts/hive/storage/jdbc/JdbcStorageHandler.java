/*
 * Copyright 2012-2014 Qubit Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qubitproducts.hive.storage.jdbc;

import com.qubitproducts.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

import java.util.*;

public class JdbcStorageHandler extends DefaultStorageHandler implements
        HiveStoragePredicateHandler {//implements HiveStorageHandler {
    private static final Log LOG = LogFactory.getLog(JdbcStorageHandler.class);

    private Configuration conf;


    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }


    @Override
    public Configuration getConf() {
        return this.conf;
    }


    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return JdbcInputFormat.class;
    }


    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return JdbcOutputFormat.class;
    }


    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return JdbcSerDe.class;
    }


    @Override
    public HiveMetaHook getMetaHook() {
        return null;
    }


    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        JdbcStorageConfigManager.copyConfigurationToJob(properties, jobProperties);
    }


    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        JdbcStorageConfigManager.copyConfigurationToJob(properties, jobProperties);
    }


    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        // Nothing to do here...
    }


    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

/*
* from https://github.com/qubole/Hive-JDBC-Storage-Handler
* org/apache/hadoop/hive/jdbc/storagehandler/JdbcStorageHandler.java
* */
    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf,
                                                  Deserializer deserializer, ExprNodeDesc predicate) {

        IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
        // adding Comaprison Operators
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd");

        // getting all column names
        String columnNames = jobConf
                .get(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS);

        StringTokenizer st = new StringTokenizer(columnNames, ",");
        // adding allowed column names
        while (st.hasMoreTokens()) {
            String columnName = (String) st.nextToken();
            analyzer.allowColumnName(columnName);
            LOG.info(columnName);
        }
        // filtering out residual and pushed predicate
        List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
        ExprNodeDesc residualPredicate = analyzer.analyzePredicate(predicate,
                searchConditions);

        for (IndexSearchCondition e : searchConditions) {
            LOG.info("COnditions fetched are: " + e.toString());
        }

        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
        decomposedPredicate.pushedPredicate = (ExprNodeGenericFuncDesc) analyzer
                .translateSearchConditions(searchConditions);
        decomposedPredicate.residualPredicate = (ExprNodeGenericFuncDesc) residualPredicate;
        if (decomposedPredicate.pushedPredicate != null)
            LOG.info("Predicates pushed: "
                    + decomposedPredicate.pushedPredicate.getExprString());
        if (decomposedPredicate.residualPredicate != null)
            LOG.info("Predicates not Pushed: "
                    + decomposedPredicate.residualPredicate.getExprString());
        return decomposedPredicate;
    }

}
