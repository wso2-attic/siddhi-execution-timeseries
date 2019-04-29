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
import io.siddhi.annotation.ReturnAttribute;
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
 * The outlier function takes in a dependent event stream (Y), an independent event stream (X) and
 * a user specified range for outliers, and returns whether the current event is an outlier,
 * based on the regression equation that fits historical data.
 */

@Extension(
        name = "outlier",
        namespace = "timeseries",
        description = "This allows the user to specify a batch size (optional) that defines the number of events " +
                "to be considered for the calculation of regression while finding the outliers." +
                "\nThis function should be used in one of the following formats." +
                "\noutlier(range, Y, X)" +
                "\nor" +
                "\noutlier(calculation interval, batch size, confidence interval, range, Y, X). There can be " +
                "different outputs and β coefficients of the regression equation and can return dynamic " +
                "attributes as beta1 , beta2 ... betan.",
        parameters = {
                @Parameter(name = "batch.size",
                        description = "The maximum number of events that could be used for a regression calculation.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "100000"),
                @Parameter(name = "range",
                        description = "The number of standard deviations from the regression calculation.",
                        type = {DataType.INT, DataType.LONG},
                        optional = true,
                        defaultValue = "0.95"),
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
        returnAttributes = {
                @ReturnAttribute(
                        name = "outlier",
                        description = "This returns 'True' if the event is an outlier and 'False' if not.",
                        type = {DataType.BOOL}),
                @ReturnAttribute(
                        name = "stderr",
                        description = "The standard error of the regression equation.",
                        type = {DataType.DOUBLE}),
                @ReturnAttribute(
                        name = "beta0",
                        description = "β coefficients of the regression equation.",
                        type = {DataType.DOUBLE}),
                @ReturnAttribute(
                        name = "beta1",
                        description = "β coefficients of the regression equation.",
                        type = {DataType.DOUBLE}),
                @ReturnAttribute(
                        name = "betan",
                        description = "β coefficients of the regression equation.",
                        type = {DataType.DOUBLE})
        },
        examples = {
                @Example(
                        syntax = "from StockExchangeStream#timeseries:outlier(2, Y, X)\n" +
                                "select *\n" +
                                "insert into StockForecaster;",
                        description = "This query submits the number of standard deviations to be used as" +
                                " a range (2), a dependent input stream (Y) and" +
                                " an independent input stream (X) that are used to" +
                                " perform linear regression between Y and X." +
                                " It returns an output that indicates whether the current event is an outlier or not."
                )
        }
)
public class LinearRegressionOutlierStreamProcessor extends StreamProcessor<State> {

    private int paramCount = 0;                               // Number of x variables +1
    private int calcInterval = 1;                             // The frequency of regression calculation
    private int batchSize = 1000000000;                       // Maximum # of events, used for regression calculation
    private double ci = 0.95;                                 // Confidence Interval
    private RegressionCalculator regressionCalculator = null;
    private int paramPosition = 1;
    private Object[] coefficients;
    private ArrayList<Attribute> attributes = new ArrayList<Attribute>(paramCount + 1);

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                Boolean result = false; // Becomes true if its an outlier

                Object[] inputData = new Object[attributeExpressionLength - paramPosition];
                double range = ((Number) attributeExpressionExecutors[paramPosition - 1]
                        .execute(complexEvent)).doubleValue();

                for (int i = paramPosition; i < attributeExpressionLength; i++) {
                    inputData[i - paramPosition] = attributeExpressionExecutors[i].execute(complexEvent);
                }

                if (coefficients != null) {
                    // Get the current Y value and X value
                    double nextY = ((Number) inputData[0]).doubleValue();
                    double nextX = ((Number) inputData[1]).doubleValue();

                    // Get the last computed regression coefficients
                    double stdError = ((Number) coefficients[0]).doubleValue();
                    double beta0 = ((Number) coefficients[1]).doubleValue();
                    double beta1 = ((Number) coefficients[2]).doubleValue();

                    // Forecast Y based on current coefficients and next X value
                    double forecastY = beta0 + beta1 * nextX;

                    // Create the normal range based on user provided range parameter and current std error
                    double upLimit = forecastY + range * stdError;
                    double downLimit = forecastY - range * stdError;

                    // Check whether next Y value is an outlier based on the next X value
                    // and the current regression equation
                    if (nextY < downLimit || nextY > upLimit) {
                        result = true;
                    }
                }
                // Perform regression including X and Y of current event
                coefficients = regressionCalculator.calculateLinearRegression(inputData);

                if (coefficients == null) {
                    streamEventChunk.remove();
                } else {
                    Object[] outputData = new Object[coefficients.length + 1];
                    System.arraycopy(coefficients, 0, outputData, 0, coefficients.length);
                    outputData[coefficients.length] = result;
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
        final int simpleLinregInputParamCount = 2;    // Number of Input parameters in a simple linear forecast
        paramCount = attributeExpressionLength - 1;

        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            paramCount = paramCount - 3;
            paramPosition = 4;
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
            throw new SiddhiAppCreationException("Outlier Function is available only for simple linear regression");
        } else {
            regressionCalculator = new SimpleLinearRegressionCalculator(paramCount, calcInterval, batchSize, ci);
        }

        // Create attributes for standard error and all beta values and the outlier result
        String betaVal;
        attributes.add(new Attribute("stderr", Attribute.Type.DOUBLE));

        for (int itr = 0; itr < paramCount; itr++) {
            betaVal = "beta" + itr;
            attributes.add(new Attribute(betaVal, Attribute.Type.DOUBLE));
        }
        attributes.add(new Attribute("outlier", Attribute.Type.BOOL));
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
