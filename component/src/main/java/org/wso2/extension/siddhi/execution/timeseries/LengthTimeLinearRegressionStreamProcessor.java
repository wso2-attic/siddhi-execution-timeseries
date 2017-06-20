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

import org.wso2.extension.siddhi.execution.timeseries.linreg.LengthTimeMultipleLinearRegression;
import org.wso2.extension.siddhi.execution.timeseries.linreg.LengthTimeRegressionCalculator;
import org.wso2.extension.siddhi.execution.timeseries.linreg.LengthTimeSimpleLinearRegressionCalculator;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
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
 * This class performs linear regression.
 * Number of data points could be constrained using both time and length windows.
 */

@Extension(
        name = "lengthTimeRegress",
        namespace = "timeseries",
        description = "This allows user to specify the time window and batch size (required). " +
                "The number of events considered for the regression calculation can be restricted based" +
                " on the time window and/or the batch size.",
        parameters = {
                @Parameter(name = "time.window",
                        description = "The maximum time duration to be considered for the regression calculation.",
                        type = {DataType.LONG}),
                @Parameter(name = "batch.size",
                        description = "The maximum number of events that shoukd be used for a regression calculation.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "1000000000"),
                @Parameter(name = "calculation.interval",
                        description = "The frequency with which the regression calculation should be carried out.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "1"),
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
                        syntax = "from StockExchangeStream#timeseries:lengthTimeRegress(200, 10000, 2, 0.95, Y, X)\n" +
                                "select *\n" +
                                "insert into StockForecaster;",
                        description =  "This  query submits" +
                                " a time window (200 milliseconds)," +
                                " a batch size (10,000 events)," +
                                " a calculation interval (every 2 events)," +
                                " a confidence interval (0.95)," +
                                " a dependent input stream (Y) and" +
                                " an independent input stream (X) that are used to perform linear regression" +
                                " between Y and all the X streams."
                )
        }
)
public class LengthTimeLinearRegressionStreamProcessor extends StreamProcessor {
    private static final int SIMPLE_LINREG_INPUT_PARAM_COUNT = 2; //Number of input parameters in
    private int paramCount; // Number of x variables +1
    private long duration; // Time window to consider for regression calculation
    private int calcInterval = 1; // The frequency of regression calculation
    private double ci = 0.95; // Confidence Interval
    // simple linear regression
    private LengthTimeRegressionCalculator regressionCalculator = null;
    private int yParameterPosition;
    // simple linear regression

    /**
     * The init method of the LinearRegressionStreamProcessor.
     * this method will be called before other methods
     *
     * @param abstractDefinition  the incoming stream definition
     * @param expressionExecutors the executors of each function parameters
     * @param siddhiAppContext    the context of the execution plan
     * @return the additional output attributes introduced by the function
     */
    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

        paramCount = attributeExpressionLength - 2; // First two events are time, length windows.
        yParameterPosition = 2;
        // Capture duration
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                duration = (Integer) ((ConstantExpressionExecutor)
                        attributeExpressionExecutors[0]).getValue();
            } else if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {
                duration = (Long) ((ConstantExpressionExecutor)
                        attributeExpressionExecutors[0]).getValue();
            } else {
                throw new SiddhiAppCreationException(
                        "Time duration parameter should be either int or long, but found "
                                + attributeExpressionExecutors[0].getReturnType());
            }
        } else {
            throw new SiddhiAppCreationException("Time duration parameter must be a constant");
        }
        // Capture batchSize
        int batchSize; // Maximum # of events, used for regression calculation
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                batchSize = (Integer)
                        ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new SiddhiAppCreationException("Size parameter should be int, but found "
                        + attributeExpressionExecutors[1].getReturnType());
            }
        } else {
            throw new SiddhiAppCreationException("Size parameter must be a constant");
        }
        // Capture calculation interval and ci if provided by user
        // Default values would be used otherwise
        if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            paramCount = paramCount - 2; // When calcInterval and ci are given by user,
            // parameter count must exclude those two as well
            yParameterPosition = 4;
            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                calcInterval = (Integer)
                        ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new SiddhiAppCreationException("Calculation interval should be " +
                        "int, but found "
                        + attributeExpressionExecutors[2].getReturnType());
            }
            if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.DOUBLE) {
                    ci = (Double) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[3]).getValue();
                    if (!(0 <= ci && ci <= 1)) {
                        throw new SiddhiAppCreationException(
                                "Confidence interval should be a value between 0 and 1");
                    }
                } else {
                    throw new SiddhiAppCreationException("Confidence interval should " +
                            "be double, but found "
                            + attributeExpressionExecutors[3].getReturnType());
                }
            } else {
                throw new SiddhiAppCreationException("Confidence interval must be a constant");
            }
        }
        // Pick the appropriate regression calculator
        if (paramCount > SIMPLE_LINREG_INPUT_PARAM_COUNT) {
            regressionCalculator = new LengthTimeMultipleLinearRegression(paramCount,
                    duration, batchSize, calcInterval, ci);
        } else {
            regressionCalculator = new LengthTimeSimpleLinearRegressionCalculator(paramCount,
                    duration, batchSize, calcInterval, ci);
        }
        // Add attributes for standard error and all beta values
        String betaVal;
        List<Attribute> attributes = new ArrayList<Attribute>(paramCount + 1);
        attributes.add(new Attribute("stderr", Attribute.Type.DOUBLE));
        for (int itr = 0; itr < paramCount; itr++) {
            betaVal = "beta" + itr;
            attributes.add(new Attribute(betaVal, Attribute.Type.DOUBLE));
        }
        return attributes;
    }

    /**
     * The main processing method that will be called upon event arrival.
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or
     *                              modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
                long eventExpiryTime = currentTime + duration;
                Object[] inputData = new Object[paramCount];
                for (int i = yParameterPosition; i < attributeExpressionLength; i++) {
                    inputData[i - yParameterPosition] = attributeExpressionExecutors[i].execute(streamEvent);
                }
                Object[] outputData = regressionCalculator.calculateLinearRegression(inputData,
                        eventExpiryTime);
                // Skip processing if user has specified calculation interval
                if (outputData == null) {
                    streamEventChunk.remove();
                } else {
                    complexEventPopulater.populateComplexEvent(streamEvent, outputData);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    /**
     * This will be called only once and this can be used to acquire required resources for the
     * processing element.
     * This will be called after initializing the system and before starting to process the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once and this can be used to release the acquired resources
     * for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for reconstructing the element to the same state at a different point of time.
     *
     * @return stateful objects of the processing element as an array
     */

    @Override
    public synchronized Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<String, Object>();
        return state;
    }

    @Override
    public synchronized void restoreState(Map<String, Object> state) {

    }
}
