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
import io.siddhi.core.event.ComplexEvent;
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
import org.wso2.extension.siddhi.execution.timeseries.linreg.RegressionCalculator;
import org.wso2.extension.siddhi.execution.timeseries.linreg.SimpleLinearRegressionCalculator;

import java.util.ArrayList;
import java.util.List;

/**
 * This class performs linear regression forecasting.
 */

@Extension(
        name = "forecast",
        namespace = "timeseries",
        description = "This allows the user to specify a batch size (optional) that defines the number of events " +
                "to be considered for the regression calculation when forecasting the Y value.",
        parameters = {
                @Parameter(name = "calculation.interval",
                        description = "The frequency with which the regression calculation should be carried out.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "1"),
                @Parameter(name = "batch.size",
                        description = "The maximum number of events that could be used for a regression calculation.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "1000000000"),
                @Parameter(name = "confidence.interval",
                        description = "The confidence interval to be used for a regression calculation.",
                        optional = true,
                        defaultValue = "0.95",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "next.x.value",
                        description = "The value to be used to forecast the Y value. This can be a constant " +
                                "or an expression (e.g., x+5).",
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
                        syntax = "from StockExchangeStream#timeseries:forecast(X+5, Y, X)\n" +
                                "select *\n" +
                                "insert into StockForecaster",
                        description = "This query submits an expression to be used as the next X value (X+2)," +
                                " a dependent input stream (Y) and an independent input stream (X) that are used" +
                                " to perform a linear regression between Y and X streams, and" +
                                " compute the forecast of Y value based on the next X value specified by the user."
                )
        }
)
public class LinearRegressionForecastStreamProcessor extends StreamProcessor<State> {

    private int paramCount = 0;                                // Number of x variables +1
    private int calcInterval = 1;                              // The frequency of regression calculation
    private int batchSize = 10000;                             // Maximum # of events, used for regression calculation
    private double ci = 0.9;                                  // Confidence Interval
    private RegressionCalculator regressionCalculator = null;
    private ArrayList<Attribute> attributes = new ArrayList<Attribute>(paramCount + 1);
    private int paramPosition = 0;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {

        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();

                Object[] inputData = new Object[attributeExpressionLength - paramPosition];

                // Obtain x value that user wants to use to forecast Y
                double xDash = ((Number) attributeExpressionExecutors[paramPosition - 1]
                        .execute(complexEvent)).doubleValue();
                for (int i = paramPosition; i < attributeExpressionLength; i++) {
                    inputData[i - paramPosition] = attributeExpressionExecutors[i].execute(complexEvent);
                }
                Object[] coefficients = regressionCalculator.calculateLinearRegression(inputData);

                if (coefficients == null) {
                    streamEventChunk.remove();
                } else {
                    Object[] outputData = new Object[coefficients.length + 1];
                    System.arraycopy(coefficients, 0, outputData, 0, coefficients.length);

                    // Calculating forecast Y based on regression equation and given x
                    outputData[coefficients.length] = ((Number) coefficients[coefficients.length - 2]).doubleValue()
                            + ((Number) coefficients[coefficients.length - 1]).doubleValue() * xDash;
                    complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }


    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition abstractDefinition,
                                       ExpressionExecutor[] expressionExecutors, ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                       SiddhiQueryContext siddhiQueryContext) {
        final int simpleLinregInputParamCount = 2;     // Number of Input parameters in a simple linear forecast

        paramCount = attributeExpressionLength;

        // Capture Constant inputs
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            paramCount = paramCount - 4;
            paramPosition = 4;
            try {
                calcInterval = ((Integer) attributeExpressionExecutors[0].execute(null));
                batchSize = ((Integer) attributeExpressionExecutors[1].execute(null));
            } catch (ClassCastException c) {
                throw new SiddhiAppCreationException("Calculation interval, " +
                        "batch size and range should be of type int");
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
            throw new SiddhiAppCreationException("Forecast Function is available only for simple linear regression");
        } else {
            regressionCalculator = new SimpleLinearRegressionCalculator(paramCount, calcInterval, batchSize, ci);
        }

        // Create attributes for standard error and all beta values and the Forecast Y value
        String betaVal;
        attributes.add(new Attribute("stderr", Attribute.Type.DOUBLE));

        for (int itr = 0; itr < paramCount; itr++) {
            betaVal = "beta" + itr;
            attributes.add(new Attribute(betaVal, Attribute.Type.DOUBLE));
        }
        attributes.add(new Attribute("forecastY", Attribute.Type.DOUBLE));
        return null;
    }

    @Override
    public void start() {

    }

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
