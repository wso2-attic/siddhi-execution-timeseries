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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.wso2.extension.siddhi.execution.timeseries.linreg.LengthTimeRegressionCalculator;
import org.wso2.extension.siddhi.execution.timeseries.linreg.LengthTimeSimpleLinearRegressionCalculator;

import java.util.ArrayList;
import java.util.List;

/**
 * Sample Query1 (time window, length window, nextX, y, x):
 * from InputStream#timeseries:lengthTimeOutlier(20 min, 20, x+2, y, x)
 * select *
 * insert into OutputStream;
 * <p>
 * Sample Query2 (time window, length window, nextX, calculation interval, confidence interval, y, x):
 * from InputStream#timeseries:lengthTimeOutlier(20 min, 20, x+2, 2, 0.9, y, x)
 * select *
 * insert into OutputStream;
 * <p>
 * This class performs enables users to forecast future events using linear regression
 * Number of data points could be constrained using both time and length windows.
 */

@Extension(
        name = "lengthTimeForecast",
        namespace = "timeseries",
        description = "This allows the user to restrict the number of events considered for the regression " +
                "calculation when forecasting the Y value based on a specified time window and/or batch size.",
        parameters = {
                @Parameter(name = "time.window",
                        description = "The maximum time duration that should be considered for " +
                                "a regression calculation.",
                        type = {DataType.LONG}),
                @Parameter(name = "batch.size",
                        description = "The maximum number of events that could be used for a regression calculation.",
                        type = {DataType.INT}),
                @Parameter(name = "next.x.value",
                        description = "The value to be used to forecast the Y value. This can be a constant " +
                                "or an expression (e.g., x+5).",
                        type = {DataType.DOUBLE}),
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
                        description = "The data stream of the independent variable.",
                        type = {DataType.DOUBLE})
        },
        examples = {
                @Example(
                        syntax = "from StockExchangeStream#timeseries:lengthTimeForecast(2 sec, 100, 10, Y, X)\n" +
                                "select *\n" +
                                "insert into StockForecaster",
                        description = "This query submits a time window (2 seconds), a batch size (100 events), " +
                                "a constant to be used as the next X value (10), a dependent input stream (Y) and " +
                                "an independent input stream (X). These inputs are used to perform linear regression " +
                                "between Y and X streams, and compute the forecast of Y value based on " +
                                "the next X value specified by the user."
                )
        }
)
public class LengthTimeLinearRegressionForecastStreamProcessor extends StreamProcessor<State> {
    private static final int SIMPLE_LINREG_INPUT_PARAM_COUNT = 2; //Number of input parameters in
    private int paramCount; // Number of x variables +1
    private long duration; // Time window to consider for regression calculation
    private int calcInterval = 1; // The frequency of regression calculation
    private double ci = 0.95; // Confidence Interval simple linear forecast
    private LengthTimeRegressionCalculator regressionCalculator = null;
    private List<Attribute> attributes;
    private int yParameterPosition;
    // simple linear regression

    /**
     * The init method of the LinearRegressionOutlierStreamProcessor.
     * This method will be called before other methods
     *
     * @param abstractDefinition  the incoming stream definition
     * @param expressionExecutors the executors of each function parameters
     * @param siddhiQueryContext  siddhi query context
     * @return the additional output attributes introduced by the function
     */
    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition abstractDefinition,
                                       ExpressionExecutor[] expressionExecutors, ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean b, boolean b1,
                                       SiddhiQueryContext siddhiQueryContext) {
        paramCount = attributeExpressionLength - 3; // First three events are time window, length
        // window and x value for forecasting y
        yParameterPosition = 3;
        // Capture Constant inputs
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
                batchSize = (Integer) ((ConstantExpressionExecutor)
                        attributeExpressionExecutors[1]).getValue();
            } else {
                throw new SiddhiAppCreationException
                        ("Size parameter should be int, but found "
                                + attributeExpressionExecutors[1].getReturnType());
            }
        } else {
            throw new SiddhiAppCreationException("Size parameter must be a constant");
        }
        // Capture calculation interval and ci if provided by user
        // Default values would be used otherwise
        if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
            paramCount = paramCount - 2; // When calcInterval and ci are given by user,
            // parameter count must exclude those two as well
            yParameterPosition = 5;
            if (attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT) {
                calcInterval = (Integer) ((ConstantExpressionExecutor)
                        attributeExpressionExecutors[3]).getValue();
            } else {
                throw new SiddhiAppCreationException
                        ("Calculation interval should be int, but found "
                                + attributeExpressionExecutors[3].getReturnType());
            }
            if (attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[4].getReturnType() == Attribute.Type.DOUBLE) {
                    ci = (Double) ((ConstantExpressionExecutor)
                            attributeExpressionExecutors[4]).getValue();
                    if (!(0 <= ci && ci <= 1)) {
                        throw new SiddhiAppCreationException
                                ("Confidence interval should be a value between 0 and 1");
                    }
                } else {
                    throw new SiddhiAppCreationException
                            ("Confidence interval should be double, but found "
                                    + attributeExpressionExecutors[4].getReturnType());
                }
            } else {
                throw new SiddhiAppCreationException("Confidence interval must be a constant");
            }
        }
        // Pick the appropriate regression calculator
        if (paramCount > SIMPLE_LINREG_INPUT_PARAM_COUNT) {
            throw new SiddhiAppCreationException(
                    "Forecast Function is available only for simple linear regression");
        } else {
            regressionCalculator = new LengthTimeSimpleLinearRegressionCalculator(paramCount,
                    duration, batchSize, calcInterval, ci);
        }
        // Create attributes for standard error and all beta values and the Forecast Y value
        String betaVal;
        attributes = new ArrayList<Attribute>(paramCount + 2);
        attributes.add(new Attribute("stderr", Attribute.Type.DOUBLE));
        for (int itr = 0; itr < paramCount; itr++) {
            betaVal = "beta" + itr;
            attributes.add(new Attribute(betaVal, Attribute.Type.DOUBLE));
        }
        attributes.add(new Attribute("forecastY", Attribute.Type.DOUBLE));
        return null;
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
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
                long eventExpiryTime = currentTime + duration;
                Object[] inputData = new Object[paramCount];
                // Obtain position of next x value (xDashPosition)
                final int xDashPosition;
                xDashPosition = 2;
                // Obtain x value (xDash) that user wants to use to forecast Y
                // This could be a constant or an expression
                double xDash =
                        ((Number) attributeExpressionExecutors[xDashPosition].execute(streamEvent)).doubleValue();
                for (int i = yParameterPosition; i < attributeExpressionLength; i++) {
                    inputData[i - yParameterPosition] =
                            attributeExpressionExecutors[i].execute(streamEvent);
                }
                Object[] coefficients = regressionCalculator.calculateLinearRegression(inputData,
                        eventExpiryTime);
                if (coefficients == null) {
                    streamEventChunk.remove();
                } else {
                    Object[] outputData = new Object[coefficients.length + 1];
                    System.arraycopy(coefficients, 0, outputData, 0, coefficients.length);
                    // Calculating forecast Y based on regression equation and given x
                    outputData[coefficients.length] =
                            ((Number) coefficients[coefficients.length - 2]).doubleValue() +
                                    ((Number) coefficients[coefficients.length - 1]).doubleValue() * xDash;
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

    @Override
    public List<Attribute> getReturnAttributes() {
        return attributes;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }
}
