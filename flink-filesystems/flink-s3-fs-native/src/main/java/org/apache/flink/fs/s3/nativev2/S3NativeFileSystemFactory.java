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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.IOException;
import java.net.URI;

/** Minimal AWS SDK v2 based S3 FileSystem factory (no Hadoop). */
public class S3NativeFileSystemFactory implements FileSystemFactory {

    private Configuration flinkConfig;

    @Override
    public void configure(Configuration config) {
        this.flinkConfig = config;
    }

    @Override
    public String getScheme() {
        return "s3";
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        // Build S3 client according to configuration
        S3Client s3 = buildClient(flinkConfig);
        return new S3NativeFileSystem(flinkConfig, fsUri, s3);
    }

    private static S3Client buildClient(Configuration config) {
        S3Configuration.Builder s3cfg =
                S3Configuration.builder()
                        .pathStyleAccessEnabled(
                                Boolean.TRUE.equals(config.get(NativeS3Options.PATH_STYLE_ACCESS)));

        S3ClientBuilder builder =
                S3Client.builder()
                        .credentialsProvider(DefaultCredentialsProvider.create())
                        .serviceConfiguration(s3cfg.build());

        String region = config.get(NativeS3Options.REGION);
        if (region != null && !region.isEmpty()) {
            builder = builder.region(software.amazon.awssdk.regions.Region.of(region));
        }
        String endpoint = config.get(NativeS3Options.ENDPOINT);
        if (endpoint != null && !endpoint.isEmpty()) {
            builder = builder.endpointOverride(URI.create(endpoint));
        }
        return builder.build();
    }
}
