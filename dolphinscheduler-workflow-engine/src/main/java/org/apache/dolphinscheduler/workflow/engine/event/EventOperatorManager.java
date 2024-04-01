/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.workflow.engine.event;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * The event operator manager interface used to get {@link ITaskEventOperator}.
 */
@Slf4j
public class EventOperatorManager implements IEventOperatorManager<IEvent> {

    private static final Map<IEventType, IEventOperator<IEvent>> EVENT_OPERATOR_MAP = new HashMap<>();

    private static final EventOperatorManager INSTANCE = new EventOperatorManager();

    private EventOperatorManager() {
    }

    public static EventOperatorManager getInstance() {
        return INSTANCE;
    }

    public void registerEventOperator(IEventType eventType, IEventOperator<IEvent> eventOperator) {
        EVENT_OPERATOR_MAP.put(eventType, eventOperator);
    }

    @Override
    public IEventOperator<IEvent> getEventOperator(IEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event cannot be null");
        }
        if (event.getEventType() == null) {
            throw new IllegalArgumentException("event operator class cannot be null");
        }
        return EVENT_OPERATOR_MAP.get(event.getEventType());
    }

}
