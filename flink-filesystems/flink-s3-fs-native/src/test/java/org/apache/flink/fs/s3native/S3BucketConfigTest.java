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

package org.apache.flink.fs.s3native;

import org.apache.flink.configuration.IllegalConfigurationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3BucketConfig}. */
class S3BucketConfigTest {

    @Test
    void testBuilderWithAllProperties() {
        S3BucketConfig config =
                S3BucketConfig.builder("my-bucket")
                        .region("us-west-2")
                        .endpoint("https://custom.s3.endpoint")
                        .pathStyleAccess(true)
                        .accessKey("AKIAIOSFODNN7EXAMPLE")
                        .secretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                        .sseType("sse-kms")
                        .sseKmsKeyId("arn:aws:kms:us-east-1:123:key/abc")
                        .assumeRoleArn("arn:aws:iam::123:role/S3Role")
                        .assumeRoleExternalId("ext-id-123")
                        .assumeRoleSessionName("my-session")
                        .assumeRoleSessionDurationSeconds(7200)
                        .credentialsProvider("AnonymousCredentialsProvider")
                        .build();

        assertThat(config.getBucketName()).isEqualTo("my-bucket");
        assertThat(config.getRegion()).isEqualTo("us-west-2");
        assertThat(config.getEndpoint()).isEqualTo("https://custom.s3.endpoint");
        assertThat(config.getPathStyleAccess()).isTrue();
        assertThat(config.getAccessKey()).isEqualTo("AKIAIOSFODNN7EXAMPLE");
        assertThat(config.getSecretKey()).isEqualTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        assertThat(config.getSseType()).isEqualTo("sse-kms");
        assertThat(config.getSseKmsKeyId()).isEqualTo("arn:aws:kms:us-east-1:123:key/abc");
        assertThat(config.getAssumeRoleArn()).isEqualTo("arn:aws:iam::123:role/S3Role");
        assertThat(config.getAssumeRoleExternalId()).isEqualTo("ext-id-123");
        assertThat(config.getAssumeRoleSessionName()).isEqualTo("my-session");
        assertThat(config.getAssumeRoleSessionDurationSeconds()).isEqualTo(7200);
        assertThat(config.getCredentialsProvider()).isEqualTo("AnonymousCredentialsProvider");
        assertThat(config.hasAnyOverride()).isTrue();
    }

    @Test
    void testBuilderWithMinimalProperties() {
        S3BucketConfig config =
                S3BucketConfig.builder("my-bucket").endpoint("http://localhost:9000").build();

        assertThat(config.getBucketName()).isEqualTo("my-bucket");
        assertThat(config.getEndpoint()).isEqualTo("http://localhost:9000");
        assertThat(config.getRegion()).isNull();
        assertThat(config.getPathStyleAccess()).isNull();
        assertThat(config.getAccessKey()).isNull();
        assertThat(config.getSecretKey()).isNull();
        assertThat(config.getSseType()).isNull();
        assertThat(config.getSseKmsKeyId()).isNull();
        assertThat(config.getAssumeRoleArn()).isNull();
        assertThat(config.getCredentialsProvider()).isNull();
        assertThat(config.hasAnyOverride()).isTrue();
    }

    @Test
    void testNoOverrides() {
        S3BucketConfig config = S3BucketConfig.builder("empty-bucket").build();

        assertThat(config.hasAnyOverride()).isFalse();
    }

    @Test
    void testPartialCredentialsAccessKeyOnly() {
        assertThatThrownBy(
                        () ->
                                S3BucketConfig.builder("my-bucket")
                                        .accessKey("AKIAIOSFODNN7EXAMPLE")
                                        .build())
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("access-key")
                .hasMessageContaining("secret-key")
                .hasMessageContaining("must be set together");
    }

    @Test
    void testPartialCredentialsSecretKeyOnly() {
        assertThatThrownBy(
                        () ->
                                S3BucketConfig.builder("my-bucket")
                                        .secretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                                        .build())
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("access-key")
                .hasMessageContaining("secret-key")
                .hasMessageContaining("must be set together");
    }

    @Test
    void testNullBucketNameThrows() {
        assertThatThrownBy(() -> S3BucketConfig.builder(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEmptyBucketNameThrows() {
        assertThatThrownBy(() -> S3BucketConfig.builder(""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEqualsAndHashCode() {
        S3BucketConfig config1 =
                S3BucketConfig.builder("bucket-a")
                        .region("us-east-1")
                        .endpoint("http://s3.local")
                        .build();
        S3BucketConfig config2 =
                S3BucketConfig.builder("bucket-a")
                        .region("us-east-1")
                        .endpoint("http://s3.local")
                        .build();
        S3BucketConfig config3 =
                S3BucketConfig.builder("bucket-b")
                        .region("us-east-1")
                        .endpoint("http://s3.local")
                        .build();

        assertThat(config1).isEqualTo(config2);
        assertThat(config1.hashCode()).isEqualTo(config2.hashCode());
        assertThat(config1).isNotEqualTo(config3);
    }

    @Test
    void testToStringRedactsCredentials() {
        S3BucketConfig config =
                S3BucketConfig.builder("secure-bucket")
                        .accessKey("AKIAIOSFODNN7EXAMPLE")
                        .secretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                        .region("us-east-1")
                        .build();

        String str = config.toString();
        assertThat(str).contains("credentials=<set>");
        assertThat(str).doesNotContain("AKIAIOSFODNN7EXAMPLE");
        assertThat(str).doesNotContain("wJalrXUtnFEMI");
    }

    @Test
    void testBothCredentialsSetIsValid() {
        S3BucketConfig config =
                S3BucketConfig.builder("my-bucket")
                        .accessKey("AKIAIOSFODNN7EXAMPLE")
                        .secretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
                        .build();

        assertThat(config.getAccessKey()).isEqualTo("AKIAIOSFODNN7EXAMPLE");
        assertThat(config.getSecretKey()).isEqualTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    }
}
