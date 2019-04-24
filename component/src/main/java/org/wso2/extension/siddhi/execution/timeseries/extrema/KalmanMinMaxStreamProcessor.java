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

package org.wso2.extension.siddhi.execution.timeseries.extrema;


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
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.extension.siddhi.execution.timeseries.extrema.util.ExtremaCalculator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;


/**
 * Implementation of Kalman Min Max for siddhiQL.
 */
@Extension(
        name = "kalmanMinMax",
        namespace = "timeseries",
        description = "The kalmanMinMax function uses the kalman filter to smooth the values of the time series " +
                "within a given window, and then determine the maxima and minima of that set of values.",
        parameters = {
                @Parameter(name = "variable",
                        description = "The value of the time series to be considered for the maxima and/or " +
                                "minima detection.",
                        type = {DataType.DOUBLE, DataType.FLOAT, DataType.INT, DataType.LONG}),
                @Parameter(name = "q",
                        description = "The standard deviation of the process noise.",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "r",
                        description = "The standard deviation of the measurement noise.",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "window.size",
                        description = "The number of values to be considered for smoothing and" +
                                " determining the extremes.",
                        type = {DataType.INT}),
                @Parameter(name = "extrema.type",
                        description = "The type of extrema considered, i.e., min, max or minmax.",
                        type = {DataType.STRING}),
        },
        examples = {
                @Example(
                        syntax = "from InputStream#timeseries:kalmanMinMax(price, 0.000001,0.0001, 25, 'min')\n" +
                                "select *\n" +
                                "insert into OutputStream;",
                        description =  "This example returns the maximum values for a set of price values given."
                ),
                @Example(
                        syntax = "from InputStream#timeseries:kalmanMinMax(price, 0.000001,0.0001, 25, 'max')\n" +
                                "select *\n" +
                                "insert into OutputStream;",
                        description =  "This example returns the minimum values for a set of price values given."
                ),
                @Example(
                        syntax = "from InputStream#timeseries:kalmanMinMax(price, 0.000001,0.0001, 25, 'minmax')\n" +
                                "select *\n" +
                                "insert into OutputStream;",
                        description =  "This example returns both the minimum values and the maximum values for a " +
                                "set of price values given."
                )
        }
)
public class KalmanMinMaxStreamProcessor extends StreamProcessor<KalmanMinMaxStreamProcessor.ExtensionState> {

    ExtremaType extremaType;
    ExtremaCalculator extremaCalculator = null;
    private int[] variablePosition;
    private int windowSize = 0;
    private LinkedList<StreamEvent> eventStack = null;
    private Queue<Double> valueStack = null;
    private Queue<StreamEvent> uniqueQueue = null;
    private double q;
    private double r;
    private int minEventPos;
    private int maxEventPos;
    private List<Attribute> attributeList = new ArrayList<Attribute>();

    @Override
    protected StateFactory<ExtensionState> init(MetaStreamEvent metaStreamEvent, AbstractDefinition abstractDefinition,
                                                ExpressionExecutor[] expressionExecutors, ConfigReader configReader,
                                                StreamEventClonerHolder streamEventClonerHolder,
                                                boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 5) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to KalmanMinMaxStreamProcessor, " +
                    "required 5, but found " + attributeExpressionExecutors.length);
        }

        if (!(attributeExpressionExecutors[0].getReturnType() == Attribute.Type.DOUBLE ||
                attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.FLOAT ||
                attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG)) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 1st argument of" +
                    " KalmanMinMaxStreamProcessor, " +
                    "required " + Attribute.Type.DOUBLE + " or " + Attribute.Type.FLOAT + " or " + Attribute.Type.INT +
                    " or " +
                    Attribute.Type.LONG + " but found " + attributeExpressionExecutors[0].getReturnType().toString());
        }
        variablePosition = ((VariableExpressionExecutor) attributeExpressionExecutors[0]).getPosition();

        try {
            q = Double.parseDouble(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                    .getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 2nd argument " +
                    "of KalmanMinMaxStreamProcessor " +
                    "required " + Attribute.Type.DOUBLE + " constant, but found " + attributeExpressionExecutors[1]
                    .getReturnType().toString());
        }
        try {
            r = Double.parseDouble(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2])
                    .getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 3rd argument " +
                    "of KalmanMinMaxStreamProcessor " +
                    "required " + Attribute.Type.DOUBLE + " constant, but found " + attributeExpressionExecutors[2]
                    .getReturnType().toString());
        }
        try {
            windowSize = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[3])
                    .getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 4th argument " +
                    "of KalmanMinMaxStreamProcessor " +
                    "required " + Attribute.Type.INT + " constant, but found " + attributeExpressionExecutors[3]
                    .getReturnType().toString());
        }

        String extremeType = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[4]).getValue();

        if ("min".equalsIgnoreCase(extremeType)) {
            extremaType = ExtremaType.MIN;
        } else if ("max".equalsIgnoreCase(extremeType)) {
            extremaType = ExtremaType.MAX;
        } else {
            extremaType = ExtremaType.MINMAX;
        }
        eventStack = new LinkedList<StreamEvent>();
        valueStack = new LinkedList<Double>();
        uniqueQueue = new LinkedList<StreamEvent>();

        attributeList.add(new Attribute("extremaType", Attribute.Type.STRING));
        return () -> new ExtensionState();
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           ExtensionState extensionState) {

        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>(false);
        synchronized (this) {
            while (streamEventChunk.hasNext()) {

                StreamEvent event = streamEventChunk.next();
                streamEventChunk.remove();
                Double eventKey = (Double) event.getAttribute(variablePosition);
                extremaCalculator = new ExtremaCalculator(q, r);
                eventStack.add(event);
                valueStack.add(eventKey);

                if (eventStack.size() > windowSize) {

                    Queue<Double> output = extremaCalculator.kalmanFilter(valueStack);
                    StreamEvent maximumEvent;
                    StreamEvent minimumEvent;

                    switch (extremaType) {
                        case MINMAX:
                            maximumEvent = getMaxEvent(output);
                            minimumEvent = getMinEvent(output);
                            if (maximumEvent != null && minimumEvent != null) {
                                if (maxEventPos > minEventPos) {
                                    returnEventChunk.add(minimumEvent);
                                    returnEventChunk.add(maximumEvent);
                                } else {
                                    returnEventChunk.add(maximumEvent);
                                    returnEventChunk.add(minimumEvent);
                                }
                            } else if (maximumEvent != null) {
                                returnEventChunk.add(maximumEvent);
                            } else if (minimumEvent != null) {
                                returnEventChunk.add(minimumEvent);
                            }
                            break;
                        case MIN:
                            minimumEvent = getMinEvent(output);
                            if (minimumEvent != null) {
                                returnEventChunk.add(minimumEvent);
                            }
                            break;
                        case MAX:
                            maximumEvent = getMaxEvent(output);
                            if (maximumEvent != null) {
                                returnEventChunk.add(maximumEvent);
                            }
                            break;
                    }
                    eventStack.remove();
                    valueStack.remove();
                }
            }
        }
        if (returnEventChunk.getFirst() != null) {
            nextProcessor.process(returnEventChunk);
        }
    }

    private StreamEvent getMinEvent(Queue<Double> output) {
        // value 2 is an optimized value for stock market domain, this value may change for other domains
        Integer smoothenedMinEventPosition = extremaCalculator.findMin(output, 2);
        if (smoothenedMinEventPosition != null) {
            //value 10 is an optimized value for stock market domain, this value may change for other domains
            Integer minEventPosition = extremaCalculator.findMin(valueStack, 10);
            if (minEventPosition != null) {
                StreamEvent returnMinimumEvent = getExtremaEvent(minEventPosition);
                if (returnMinimumEvent != null) {
                    minEventPos = minEventPosition;
                    complexEventPopulater.populateComplexEvent(returnMinimumEvent, new Object[]{"min"});
                    return returnMinimumEvent;
                }
            }
        }
        return null;
    }

    private StreamEvent getMaxEvent(Queue<Double> output) {
        // value 2 is an optimized value for stock market domain, this value may change for other domains
        Integer smoothenedMaxEventPosition = extremaCalculator.findMax(output, 2);
        if (smoothenedMaxEventPosition != null) {
            //value 10 is an optimized value for stock market domain, this value may change for other domains
            Integer maxEventPosition = extremaCalculator.findMax(valueStack, 10);
            if (maxEventPosition != null) {
                StreamEvent returnMaximumEvent = getExtremaEvent(maxEventPosition);
                if (returnMaximumEvent != null) {
                    maxEventPos = maxEventPosition;
                    complexEventPopulater.populateComplexEvent(returnMaximumEvent, new Object[]{"max"});
                    return returnMaximumEvent;
                }
            }
        }
        return null;
    }

    private StreamEvent getExtremaEvent(Integer eventPosition) {
        StreamEvent extremaEvent = eventStack.get(eventPosition);
        if (!uniqueQueue.contains(extremaEvent)) {
            //value 5 is an optimized value for stock market domain, this value may change for other domains
            if (uniqueQueue.size() > 5) {
                uniqueQueue.remove();
            }
            uniqueQueue.add(extremaEvent);
            return streamEventClonerHolder.getStreamEventCloner().copyStreamEvent(extremaEvent);
        }
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
        return attributeList;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    class ExtensionState extends State {

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            synchronized (KalmanMinMaxStreamProcessor.this) {
                Map<String, Object> state = new HashMap<String, Object>();
                state.put("eventStack", eventStack);
                state.put("valueStack", valueStack);
                state.put("uniqueQueue", uniqueQueue);

                return state;
            }
        }

        @Override
        public void restore(Map<String, Object> state) {
            synchronized (KalmanMinMaxStreamProcessor.this) {
                eventStack = (LinkedList<StreamEvent>) state.get("eventStack");
                valueStack = (Queue<Double>) state.get("valueStack");
                uniqueQueue = (Queue<StreamEvent>) state.get("uniqueQueue");
            }
        }
    }

    /**
     * Enumeration for extrema types.
     */
    public enum ExtremaType {
        MIN, MAX, MINMAX
    }
}
