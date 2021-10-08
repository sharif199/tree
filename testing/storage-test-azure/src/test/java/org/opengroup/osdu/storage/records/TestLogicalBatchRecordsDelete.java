// Copyright 2017-2021, Schlumberger
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

package org.opengroup.osdu.storage.records;

import org.junit.After;
import org.junit.Before;
import org.opengroup.osdu.storage.util.AzureTestUtils;

public class TestLogicalBatchRecordsDelete extends LogicalBatchRecordsDeleteTests {

    private static final AzureTestUtils azureTestUtils = new AzureTestUtils();


    @Before
    @Override
    public void setup() throws Exception {
        this.testUtils = new AzureTestUtils();
        super.setup(azureTestUtils.getToken());
    }

    @After
    @Override
    public void tearDown() throws Exception {
        this.testUtils = null;
        super.tearDown(azureTestUtils.getToken());
    }
}
