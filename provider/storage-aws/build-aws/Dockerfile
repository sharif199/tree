# Copyright © 2020 Amazon Web Services
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# https://docs.spring.io/spring-boot/docs/current/reference/html/deployment.html
FROM amazoncorretto:8

ARG JAR_FILE=provider/storage-aws/target/*spring-boot.jar

#Default to using self signed generated TLS cert
ENV USE_SELF_SIGNED_SSL_CERT true

WORKDIR /
COPY ${JAR_FILE} app.jar
COPY /provider/storage-aws/build-aws/ssl.sh /ssl.sh
COPY /provider/storage-aws/build-aws/entrypoint.sh /entrypoint.sh
EXPOSE 8080

ENTRYPOINT ["/bin/sh", "-c", ". /entrypoint.sh"]

