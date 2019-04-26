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
 * Implementation of Kernel Min Max for siddhiQL.
 */

@Extension(
        name = "kernelMinMax",
        namespace = "timeseries",
        description = "TBD",
        parameters = {
                @Parameter(name = "variable",
                        description = "The value of the time series to be considered for the detection of minima " +
                                "and/or maxima.",
                        type = {DataType.DOUBLE, DataType.FLOAT, DataType.INT, DataType.LONG}),
                @Parameter(name = "bandwidth",
                        description = "The bandwidth of the Gaussian Kernel calculation.",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "window.size",
                        description = "The number of values to be considered for smoothing and" +
                                " determining the extremes.",
                        type = {DataType.INT}),
                @Parameter(name = "extrema.type",
                        description = "This is the type of extrema considered, i.e., min, max or minmax.",
                        type = {DataType.STRING}),
        },
        examples = {
                @Example(
                        syntax = "from InputStream#timeseries:kernelMinMax(price, 3, 7, ‘min’)\n" +
                                "select *\n" +
                                "insert into OutputStream;",
                        description =  "This example returns the maximum values for a set of price values."
                ),
                @Example(
                        syntax = "from InputStream#timeseries:kernelMinMax(price, 3, 7, 'max')\n" +
                                "select *\n" +
                                "insert into OutputStream;",
                        description =  "This example returns the minimum values for a set of price values."
                ),
                @Example(
                        syntax = "from InputStream#timeseries:kernelMinMax(price, 3, 7, ‘minmax’)\n" +
                                "select *\n" +
                                "insert into OutputStream;",
                        description =  "This example returns both the minimum values and the maximum values for a " +
                                "set of price values."
                )
        }
)
public class KernelMinMaxStreamProcessor extends StreamProcessor<KernelMinMaxStreamProcessor.ExtensionState> {

    ExtremaType extremaType;
    int[] variablePosition;
    double bw = 0;
    int windowSize = 0;
    ExtremaCalculator extremaCalculator = null;
    private int minEventPos;
    private int maxEventPos;
    private List<Attribute> attributeList = new ArrayList<Attribute>();


    @Override
    protected StateFactory<ExtensionState> init(MetaStreamEvent metaStreamEvent, AbstractDefinition abstractDefinition,
                                                ExpressionExecutor[] expressionExecutors, ConfigReader configReader,
                                                StreamEventClonerHolder streamEventClonerHolder,
                                                boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                                SiddhiQueryContext siddhiQueryContext) {

        if (attributeExpressionExecutors.length != 4) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to KernelMinMaxStreamProcessor," +
                    " required 4, " +
                    "but found " + attributeExpressionExecutors.length);
        }
        if (!(attributeExpressionExecutors[0].getReturnType() == Attribute.Type.DOUBLE ||
                attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.FLOAT ||
                attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG)) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 1st argument" +
                    " of KernelMinMaxStreamProcessor, " +
                    "required " + Attribute.Type.DOUBLE + " or " + Attribute.Type.FLOAT + " or "
                    + Attribute.Type.INT + " or " +
                    Attribute.Type.LONG + " but found " + attributeExpressionExecutors[0]
                    .getReturnType().toString());
        }

        try {
            bw = Double.parseDouble(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                    .getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 2nd argument" +
                    " of KernelMinMaxStreamProcessor " +
                    "required " + Attribute.Type.DOUBLE + " constant, but found " + attributeExpressionExecutors[1]
                    .getReturnType().toString());
        }

        if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) || attributeExpressionExecutors[2]
                .getReturnType() != Attribute.Type.INT) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 3rd argument " +
                    "of KernelMinMaxStreamProcessor, " +
                    "required " + Attribute.Type.INT + " constant, but found " + attributeExpressionExecutors[2]
                    .getReturnType().toString());
        }
        if (!(attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) || attributeExpressionExecutors[3]
                .getReturnType() != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 4th argument " +
                    "of KernelMinMaxStreamProcessor, " +
                    "required " + Attribute.Type.STRING + " constant, but found " + attributeExpressionExecutors[2]
                    .getReturnType().toString());
        }

        variablePosition = ((VariableExpressionExecutor) attributeExpressionExecutors[0]).getPosition();
        windowSize = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2])
                .getValue()));
        String extremeType = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue();

        if ("min".equalsIgnoreCase(extremeType)) {
            extremaType = ExtremaType.MIN;
        } else if ("max".equalsIgnoreCase(extremeType)) {
            extremaType = ExtremaType.MAX;
        } else {
            extremaType = ExtremaType.MINMAX;
        }
        extremaCalculator = new ExtremaCalculator();

        attributeList.add(new Attribute("extremaType", Attribute.Type.STRING));
        return () -> new ExtensionState();

    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           ExtensionState extensionState) {

        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>(false);
        synchronized (this) {
            while (streamEventChunk.hasNext()) {

                StreamEvent event = streamEventChunk.next();
                streamEventChunk.remove();
                Double eventKey = (Double) event.getAttribute(variablePosition);
                extensionState.eventStack.add(event);
                extensionState.valueStack.add(eventKey);

                if (extensionState.sizeOfEventStack() > windowSize) {
                    Queue<Double> smoothedValues = extremaCalculator.smooth(extensionState.valueStack, bw);
                    StreamEvent minimumEvent;
                    StreamEvent maximumEvent;

                    switch (extremaType) {
                        case MINMAX:
                            maximumEvent = getMaxEvent(smoothedValues, extensionState);
                            minimumEvent = getMinEvent(smoothedValues, extensionState);
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
                            minimumEvent = getMinEvent(smoothedValues, extensionState);
                            if (minimumEvent != null) {
                                returnEventChunk.add(minimumEvent);
                            }
                            break;
                        case MAX:
                            maximumEvent = getMaxEvent(smoothedValues, extensionState);
                            if (maximumEvent != null) {
                                returnEventChunk.add(maximumEvent);
                            }
                            break;
                    }
                    extensionState.removeFromEventStack();
                    extensionState.removeFromValueStack();
                }
            }
        }
        if (returnEventChunk.getFirst() != null) {
            nextProcessor.process(returnEventChunk);
        }
    }

    private StreamEvent getMaxEvent(Queue<Double> smoothedValues, ExtensionState extensionState) {
        //value 1 is an optimized value for stock market domain, this value may change for other domains
        Integer maxPosition = extremaCalculator.findMax(smoothedValues, 1);
        if (maxPosition != null) {
            //values 5 and 3 are optimized values for stock market domain, these value may change for other domains
            Integer maxEventPosition = extremaCalculator.findMax(extensionState.valueStack, windowSize / 5,
                    windowSize / 3);
            StreamEvent returnMaximumEvent = getExtremaEvent(maxPosition, maxEventPosition, extensionState);
            if (returnMaximumEvent != null) {
                maxEventPos = maxEventPosition;
                complexEventPopulater.populateComplexEvent(returnMaximumEvent, new Object[]{"max"});
                return returnMaximumEvent;
            }
        }
        return null;
    }

    private StreamEvent getMinEvent(Queue<Double> smoothedValues, ExtensionState extensionState) {
        //value 1 is an optimized value for stock market domain, this value may change for other domains
        Integer minPosition = extremaCalculator.findMin(smoothedValues, 1);
        if (minPosition != null) {
            //values 5 and 3 are optimized values for stock market domain, these value may change for other domains
            Integer minEventPosition = extremaCalculator.findMin(extensionState.valueStack, windowSize / 5,
                    windowSize / 3);
            StreamEvent returnMinimumEvent = getExtremaEvent(minPosition, minEventPosition, extensionState);
            if (returnMinimumEvent != null) {
                minEventPos = minEventPosition;
                complexEventPopulater.populateComplexEvent(returnMinimumEvent, new Object[]{"min"});
                return returnMinimumEvent;
            }
        }
        return null;
    }

    private StreamEvent getExtremaEvent(Integer smoothenedPosition, Integer eventPosition,
                                        ExtensionState extensionState) {
        //values 5 and 3 are optimized values for stock market domain, these value may change for other domains
        if (eventPosition != null && eventPosition - smoothenedPosition <= windowSize / 5 &&
                smoothenedPosition - eventPosition <= windowSize / 2) {
            StreamEvent extremaEvent = extensionState.eventStack.get(eventPosition);
            if (!extensionState.uniqueQueue.contains(extremaEvent)) {
                //value 5 is an optimized value for stock market domain, this value may change for other domains
                if (extensionState.sizeOfUniqueQueue() > 5) {
                    extensionState.removeFromUniqueQueue();
                }
                extensionState.addInUniqueQueue(extremaEvent);
                extensionState.removeFromEventStack();
                extensionState.removeFromValueStack();
                return streamEventClonerHolder.getStreamEventCloner().copyStreamEvent(extremaEvent);
            }
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

        LinkedList<StreamEvent> eventStack;
        Queue<Double> valueStack;
        Queue<StreamEvent> uniqueQueue;

        private ExtensionState() {
            eventStack = new LinkedList<StreamEvent>();
            valueStack = new LinkedList<Double>();
            uniqueQueue = new LinkedList<StreamEvent>();
        }

        private synchronized void addInUniqueQueue(StreamEvent extremaEvent) {
            uniqueQueue.add(extremaEvent);
        }

        private synchronized void removeFromUniqueQueue() {
            uniqueQueue.remove();
        }

        private synchronized void removeFromEventStack() {
            eventStack.remove();
        }

        private synchronized void removeFromValueStack() {
            valueStack.remove();
        }

        private synchronized int sizeOfUniqueQueue() {
            return uniqueQueue.size();
        }

        private synchronized int sizeOfEventStack() {
            return eventStack.size();
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            synchronized (KernelMinMaxStreamProcessor.this) {
                Map<String, Object> state = new HashMap<String, Object>();
                state.put("eventStack", eventStack);
                state.put("valueStack", valueStack);
                state.put("uniqueQueue", uniqueQueue);

                return state;
            }
        }

        @Override
        public void restore(Map<String, Object> state) {
            synchronized (KernelMinMaxStreamProcessor.this) {
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
