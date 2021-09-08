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

package org.opengroup.osdu.storage.provider.azure.di;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventGridConfig {

    private boolean publishToEventGridEnabled;

    // The Event Grid Event can be a maximum of 1MB. The batch size manipulation will impact the costing.
    // https://docs.microsoft.com/en-us/azure/event-grid/event-schema#:~:text=Event%20sources%20send%20events%20to,is%20limited%20to%201%20MB.
    private Integer eventGridBatchSize;

    private String topicName;

    public EventGridConfig(@Value("#{new Boolean('${azure.publishToEventGrid}')}") boolean publish,
                           @Value("#{new Integer('${azure.eventGridBatchSize}')}") int batchSize,
                           @Value("${azure.eventGrid.topicName}") String topicName) {
        if (publish) {
            if ((topicName.isEmpty() || batchSize <= 0)) {
                throw new RuntimeException("Missing EventGrid Configuration");
            }
        }
        this.publishToEventGridEnabled = publish;
        this.eventGridBatchSize = batchSize;
        this.topicName = topicName;
    }

    public boolean isPublishingToEventGridEnabled() {
        return publishToEventGridEnabled;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getEventGridBatchSize() {
        return eventGridBatchSize;
    }
}
