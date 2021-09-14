// Copyright © Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.opengroup.osdu.storage.provider.azure;

import org.opengroup.osdu.azure.publisherFacade.MessagePublisher;
import org.opengroup.osdu.azure.publisherFacade.PublisherInfo;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.storage.PubSubInfo;
import org.opengroup.osdu.storage.provider.azure.di.EventGridConfig;
import org.opengroup.osdu.storage.provider.azure.di.PubSubConfig;
import org.opengroup.osdu.storage.provider.azure.di.PublisherConfig;
import org.opengroup.osdu.storage.provider.interfaces.IMessageBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class MessageBusImpl implements IMessageBus {
    private final static Logger LOGGER = LoggerFactory.getLogger(MessageBusImpl.class);
    private final static String RECORDS_CHANGED_EVENT_SUBJECT = "RecordsChanged";
    private final static String RECORDS_CHANGED_EVENT_TYPE = "RecordsChanged";
    private final static String RECORDS_CHANGED_EVENT_DATA_VERSION = "1.0";
    @Autowired
    PubSubConfig pubSubConfig;
    @Autowired
    private EventGridConfig eventGridConfig;
    @Autowired
    private MessagePublisher messagePublisher;
    @Autowired
    private PublisherConfig publisherConfig;

    @Override
    public void publishMessage(DpsHeaders headers, PubSubInfo... messages) {
        // The batch size is same for both Event grid and Service bus.
        final int BATCH_SIZE = Integer.parseInt(publisherConfig.getPubSubBatchSize());
        for (int i = 0; i < messages.length; i += BATCH_SIZE) {
            PubSubInfo[] batch = Arrays.copyOfRange(messages, i, Math.min(messages.length, i + BATCH_SIZE));
            PublisherInfo publisherInfo = PublisherInfo.builder()
                    .batch(batch)
                    .eventGridTopicName(eventGridConfig.getTopicName())
                    .eventGridEventSubject(RECORDS_CHANGED_EVENT_SUBJECT)
                    .eventGridEventType(RECORDS_CHANGED_EVENT_TYPE)
                    .eventGridEventDataVersion(RECORDS_CHANGED_EVENT_DATA_VERSION)
                    .serviceBusTopicName(pubSubConfig.getServiceBusTopic())
                    .build();
            messagePublisher.publishMessage(headers, publisherInfo);
        }
    }
}