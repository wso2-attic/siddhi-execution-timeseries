/*
 * Copyright (c)  2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.timeseries;

import org.wso2.extension.siddhi.execution.timeseries.linreg.MultipleLinearRegressionCalculator;
import org.wso2.extension.siddhi.execution.timeseries.linreg.RegressionCalculator;
import org.wso2.extension.siddhi.execution.timeseries.linreg.SimpleLinearRegressionCalculator;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The methods supported by this function are
 * timeseries:regress(int/long/float/double y, int/long/float/double x1, int/long/float/double x2 ...)
 * and
 * timeseries:regress(int calcInterval, int batchSize, double confidenceInterval, int/long/float/double y,
 * int/long/float/double x1, int/long/float/double x2 ...).
 */

@Extension(
        name = "regress",
        namespace = "timeseries",
        description = "This allows the user to specify the batch size (optional) that defines the number of events" +
                " to be considered for the calculation of regression.",
        parameters = {
                @Parameter(name = "calculation.interval",
                        description = "The frequency with which the regression calculation should be carried out.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "1"),
                @Parameter(name = "batch.size",
                        description = "The maximum number of events that should be used for a regression calculation.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "1000000000"),
                @Parameter(name = "confidence.interval",
                        description = "The confidence interval to be used for a regression calculation.",
                        optional = true,
                        defaultValue = "0.95",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "y.stream",
                        description = "The data stream of the dependent variable.",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "x.stream",
                        description = "The data stream(s) of the independent variable.",
                        type = {DataType.DOUBLE})
        },
        examples = {
                @Example(
                        syntax = "from StockExchangeStream#timeseries:regress(10, 100000, 0.95, Y, X1, X2, X3)\n" +
                                "select *\n" +
                                "insert into StockForecaster",
                        description =  "This  query submits a calculation interval (every 10 events)," +
                                " a batch size (100,000 events), a confidence interval (0.95)," +
                                " a dependent input stream (Y) and" +
                                " 3 independent input streams (X1, X2, X3) that are used to perform linear regression" +
                                " between Y and all the X streams."
                )
        }
)
public class LinearRegressionStreamProcessor extends StreamProcessor {

    private int paramCount = 0;                              // Number of x variables +1
    private int calcInterval = 1;                            // The frequency of regression calculation
    private int batchSize = 1000000000;                      // Maximum # of events, used for regression calculation
    private double ci = 0.95;                                // Confidence Interval
    private RegressionCalculator regressionCalculator = null;
    private int paramPosition = 0;


    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        final int simpleLinregInputParamCount = 2;  // Number of Input parameters in a simple linear regression
        paramCount = attributeExpressionLength;

        // Capture constant inputs
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            paramCount = paramCount - 3;
            paramPosition = 3;
            try {
                calcInterval = ((Integer) attributeExpressionExecutors[0].execute(null));
                batchSize = ((Integer) attributeExpressionExecutors[1].execute(null));
            } catch (ClassCastException c) {
                throw new SiddhiAppCreationException("Calculation interval," +
                        " batch size and range should be of type int");
            }
            try {
                ci = ((Double) attributeExpressionExecutors[2].execute(null));
            } catch (ClassCastException c) {
                throw new SiddhiAppCreationException("Confidence interval should be of type double");
            }
            if (!(0 <= ci && ci <= 1)) {
                throw new SiddhiAppCreationException("Confidence interval should be a value between 0 and 1");
            }
        }

        // Pick the appropriate regression calculator
        if (paramCount > simpleLinregInputParamCount) {
            regressionCalculator = new MultipleLinearRegressionCalculator(paramCount, calcInterval, batchSize, ci);
        } else {
            regressionCalculator = new SimpleLinearRegressionCalculator(paramCount, calcInterval, batchSize, ci);
        }


        // Add attributes for standard error and all beta values
        String betaVal;
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(paramCount);
        attributes.add(new Attribute("stderr", Attribute.Type.DOUBLE));

        for (int itr = 0; itr < paramCount; itr++) {
            betaVal = "beta" + itr;
            attributes.add(new Attribute(betaVal, Attribute.Type.DOUBLE));
        }
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();

                Object[] inputData = new Object[attributeExpressionLength - paramPosition];
                for (int i = paramPosition; i < attributeExpressionLength; i++) {
                    inputData[i - paramPosition] = attributeExpressionExecutors[i].execute(complexEvent);
                }
                Object[] outputData = regressionCalculator.calculateLinearRegression(inputData);

                // Skip processing if user has specified calculation interval
                if (outputData == null) {
                    streamEventChunk.remove();
                } else {
                    complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                }
            }
        }
        nextProcessor.process(streamEventChunk);

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public synchronized Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<String, Object>();
        return state;
    }

    @Override
    public synchronized void restoreState(Map<String, Object> state) {

    }

}
