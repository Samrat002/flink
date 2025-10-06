/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.s3.nativev2;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

/** Configuration options for the native S3 file system. */
final class NativeS3Options {

    private NativeS3Options() {}

    static final ConfigOption<String> REGION =
            ConfigOptions.key("s3.region").stringType().noDefaultValue();

    static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("s3.endpoint").stringType().noDefaultValue();

    static final ConfigOption<Boolean> PATH_STYLE_ACCESS =
            ConfigOptions.key("s3.path.style.access").booleanType().defaultValue(false);

    static final ConfigOption<MemorySize> READ_CHUNK_SIZE =
            ConfigOptions.key("s3.read.chunk-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(8));

    static final ConfigOption<MemorySize> MULTIPART_PART_SIZE =
            ConfigOptions.key("s3.upload.part.size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(64));

    static final ConfigOption<MemorySize> MULTIPART_THRESHOLD =
            ConfigOptions.key("s3.upload.multipart.threshold")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(64));
}
