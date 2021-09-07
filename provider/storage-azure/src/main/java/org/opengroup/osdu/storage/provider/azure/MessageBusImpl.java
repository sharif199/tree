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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.microsoft.azure.eventgrid.models.EventGridEvent;
import com.microsoft.azure.servicebus.Message;
import org.joda.time.DateTime;
import org.opengroup.osdu.azure.eventgrid.EventGridTopicStore;
import org.opengroup.osdu.azure.publisherFacade.MessagePublisher;
import org.opengroup.osdu.azure.servicebus.ITopicClientFactory;
//import org.opengroup.osdu.azure.publisherFacade;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.storage.PubSubInfo;
import org.opengroup.osdu.storage.provider.azure.di.EventGridConfig;
import org.opengroup.osdu.storage.provider.azure.di.PubSubConfig;
import org.opengroup.osdu.storage.provider.interfaces.IMessageBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.inject.Named;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Component
public class MessageBusImpl implements IMessageBus {
    private final static Logger LOGGER = LoggerFactory.getLogger(MessageBusImpl.class);
    @Autowired
    PubSubConfig pubSubConfig;
    @Autowired
    private MessagePublisher messagePublisher;

    @Override
    public void publishMessage(DpsHeaders headers, PubSubInfo... messages) {
        // The batch size is same for both Event grid and Service bus.
        final int BATCH_SIZE = Integer.parseInt(pubSubConfig.getPubSubBatchSize());
        for (int i = 0; i < messages.length; i += BATCH_SIZE) {
            PubSubInfo[] batch = Arrays.copyOfRange(messages, i, Math.min(messages.length, i + BATCH_SIZE));
            messagePublisher.publishMessage(batch);
        }
    }
}