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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BucketConfigProvider}. */
class BucketConfigProviderTest {

    @Test
    void testParsesSingleBucketConfig() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my-bucket.endpoint", "https://s3.us-west-2.amazonaws.com");
        config.setString("s3.bucket.my-bucket.path-style-access", "true");
        config.setString("s3.bucket.my-bucket.region", "us-west-2");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.hasBucketConfig("my-bucket")).isTrue();
        assertThat(provider.size()).isEqualTo(1);

        S3BucketConfig bucketConfig = provider.getBucketConfig("my-bucket");
        assertThat(bucketConfig).isNotNull();
        assertThat(bucketConfig.getEndpoint()).isEqualTo("https://s3.us-west-2.amazonaws.com");
        assertThat(bucketConfig.getPathStyleAccess()).isTrue();
        assertThat(bucketConfig.getRegion()).isEqualTo("us-west-2");
    }

    @Test
    void testParsesMultipleBucketConfigs() {
        Configuration config = new Configuration();
        config.setString(
                "s3.bucket.checkpoint-bucket.endpoint", "https://s3.us-east-1.amazonaws.com");
        config.setString("s3.bucket.checkpoint-bucket.region", "us-east-1");
        config.setString(
                "s3.bucket.savepoint-bucket.endpoint", "https://s3.eu-west-1.amazonaws.com");
        config.setString("s3.bucket.savepoint-bucket.region", "eu-west-1");
        config.setString("s3.bucket.savepoint-bucket.path-style-access", "false");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.size()).isEqualTo(2);

        S3BucketConfig cpConfig = provider.getBucketConfig("checkpoint-bucket");
        assertThat(cpConfig).isNotNull();
        assertThat(cpConfig.getEndpoint()).isEqualTo("https://s3.us-east-1.amazonaws.com");
        assertThat(cpConfig.getRegion()).isEqualTo("us-east-1");
        assertThat(cpConfig.getPathStyleAccess()).isNull();

        S3BucketConfig spConfig = provider.getBucketConfig("savepoint-bucket");
        assertThat(spConfig).isNotNull();
        assertThat(spConfig.getEndpoint()).isEqualTo("https://s3.eu-west-1.amazonaws.com");
        assertThat(spConfig.getRegion()).isEqualTo("eu-west-1");
        assertThat(spConfig.getPathStyleAccess()).isFalse();
    }

    @Test
    void testBucketConfigWithCredentials() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.secure-bucket.access-key", "AKIAIOSFODNN7EXAMPLE");
        config.setString(
                "s3.bucket.secure-bucket.secret-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        S3BucketConfig bucketConfig = provider.getBucketConfig("secure-bucket");
        assertThat(bucketConfig).isNotNull();
        assertThat(bucketConfig.getAccessKey()).isEqualTo("AKIAIOSFODNN7EXAMPLE");
        assertThat(bucketConfig.getSecretKey())
                .isEqualTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    }

    @Test
    void testBucketConfigWithEncryption() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.encrypted-bucket.sse.type", "sse-kms");
        config.setString(
                "s3.bucket.encrypted-bucket.sse.kms-key-id",
                "arn:aws:kms:us-east-1:123456789:key/12345678");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        S3BucketConfig bucketConfig = provider.getBucketConfig("encrypted-bucket");
        assertThat(bucketConfig).isNotNull();
        assertThat(bucketConfig.getSseType()).isEqualTo("sse-kms");
        assertThat(bucketConfig.getSseKmsKeyId())
                .isEqualTo("arn:aws:kms:us-east-1:123456789:key/12345678");
    }

    @Test
    void testBucketConfigWithAssumeRole() {
        Configuration config = new Configuration();
        config.setString(
                "s3.bucket.cross-account-bucket.assume-role.arn",
                "arn:aws:iam::123456789012:role/S3AccessRole");
        config.setString("s3.bucket.cross-account-bucket.assume-role.external-id", "ext-id-abc");
        config.setString("s3.bucket.cross-account-bucket.assume-role.session-name", "flink-job");
        config.setString("s3.bucket.cross-account-bucket.assume-role.session-duration", "7200");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        S3BucketConfig bucketConfig = provider.getBucketConfig("cross-account-bucket");
        assertThat(bucketConfig).isNotNull();
        assertThat(bucketConfig.getAssumeRoleArn())
                .isEqualTo("arn:aws:iam::123456789012:role/S3AccessRole");
        assertThat(bucketConfig.getAssumeRoleExternalId()).isEqualTo("ext-id-abc");
        assertThat(bucketConfig.getAssumeRoleSessionName()).isEqualTo("flink-job");
        assertThat(bucketConfig.getAssumeRoleSessionDurationSeconds()).isEqualTo(7200);
    }

    @Test
    void testBucketConfigWithCredentialsProvider() {
        Configuration config = new Configuration();
        config.setString(
                "s3.bucket.custom-bucket.credentials.provider",
                "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        S3BucketConfig bucketConfig = provider.getBucketConfig("custom-bucket");
        assertThat(bucketConfig).isNotNull();
        assertThat(bucketConfig.getCredentialsProvider())
                .isEqualTo("software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider");
    }

    @Test
    void testDottedBucketName() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my.company.data.endpoint", "https://s3-custom.example.com");
        config.setString("s3.bucket.my.company.data.region", "ap-southeast-1");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.hasBucketConfig("my.company.data")).isTrue();

        S3BucketConfig bucketConfig = provider.getBucketConfig("my.company.data");
        assertThat(bucketConfig).isNotNull();
        assertThat(bucketConfig.getEndpoint()).isEqualTo("https://s3-custom.example.com");
        assertThat(bucketConfig.getRegion()).isEqualTo("ap-southeast-1");
    }

    @Test
    void testDottedBucketNameWithSseProperty() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my.dotted.bucket.sse.type", "sse-s3");
        config.setString("s3.bucket.my.dotted.bucket.sse.kms-key-id", "key-123");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        S3BucketConfig bucketConfig = provider.getBucketConfig("my.dotted.bucket");
        assertThat(bucketConfig).isNotNull();
        assertThat(bucketConfig.getSseType()).isEqualTo("sse-s3");
        assertThat(bucketConfig.getSseKmsKeyId()).isEqualTo("key-123");
    }

    @Test
    void testNonBucketConfigKeysIgnored() {
        Configuration config = new Configuration();
        config.setString("s3.access-key", "GLOBAL_KEY");
        config.setString("s3.secret-key", "GLOBAL_SECRET");
        config.setString("s3.region", "us-east-1");
        config.setString("s3.bucket.my-bucket.region", "eu-west-1");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.size()).isEqualTo(1);
        assertThat(provider.hasBucketConfig("my-bucket")).isTrue();
    }

    @Test
    void testUnknownBucketPropertyIgnored() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my-bucket.unknown-property", "some-value");
        config.setString("s3.bucket.my-bucket.region", "us-east-1");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        S3BucketConfig bucketConfig = provider.getBucketConfig("my-bucket");
        assertThat(bucketConfig).isNotNull();
        assertThat(bucketConfig.getRegion()).isEqualTo("us-east-1");
    }

    @Test
    void testNoBucketConfigReturnsNull() {
        Configuration config = new Configuration();
        config.setString("s3.access-key", "GLOBAL_KEY");
        config.setString("s3.secret-key", "GLOBAL_SECRET");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.getBucketConfig("non-existent-bucket")).isNull();
        assertThat(provider.size()).isEqualTo(0);
    }

    @Test
    void testEmptyConfiguration() {
        BucketConfigProvider provider = new BucketConfigProvider(new Configuration());

        assertThat(provider.size()).isEqualTo(0);
        assertThat(provider.getBucketConfig("any-bucket")).isNull();
    }

    @Test
    void testPartialCredentialsRejected() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.bad-bucket.access-key", "AKIAIOSFODNN7EXAMPLE");

        assertThatThrownBy(() -> new BucketConfigProvider(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("must be set together");
    }

    @Test
    void testInvalidSessionDurationThrowsException() {
        Configuration config = new Configuration();
        config.setString("s3.bucket.my-bucket.assume-role.session-duration", "not-a-number");
        config.setString("s3.bucket.my-bucket.region", "us-east-1");

        assertThatThrownBy(() -> new BucketConfigProvider(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Invalid assume-role.session-duration");
    }

    @Test
    void testPropertyApplicatorsCoverAllKnownProperties() {
        assertThat(BucketConfigProvider.PROPERTY_APPLICATORS.size())
                .as("PROPERTY_APPLICATORS must have an entry for every known property")
                .isEqualTo(BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.size());

        assertThat(BucketConfigProvider.PROPERTY_APPLICATORS.keySet())
                .containsExactlyInAnyOrderElementsOf(
                        BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH);
    }

    @Test
    void testKnownPropertiesSortedByDescendingLength() {
        for (int i = 1; i < BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.size(); i++) {
            assertThat(BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.get(i).length())
                    .as(
                            "Property at index %d ('%s') should not be longer than property at index %d ('%s')",
                            i,
                            BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.get(i),
                            i - 1,
                            BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.get(i - 1))
                    .isLessThanOrEqualTo(
                            BucketConfigProvider.KNOWN_PROPERTIES_BY_LENGTH.get(i - 1).length());
        }
    }

    @Test
    void testFullScenarioCheckpointAndSavepoint() {
        Configuration config = new Configuration();
        config.setString("s3.access-key", "GLOBAL_KEY");
        config.setString("s3.secret-key", "GLOBAL_SECRET");
        config.setString("s3.region", "us-east-1");

        config.setString("s3.bucket.cp-bucket.path-style-access", "true");
        config.setString("s3.bucket.cp-bucket.endpoint", "https://s3-compat.internal:9000");

        config.setString("s3.bucket.sp-bucket.access-key", "SP_ACCESS_KEY");
        config.setString("s3.bucket.sp-bucket.secret-key", "SP_SECRET_KEY");
        config.setString("s3.bucket.sp-bucket.region", "eu-west-1");
        config.setString(
                "s3.bucket.sp-bucket.assume-role.arn", "arn:aws:iam::999:role/SavepointRole");

        BucketConfigProvider provider = new BucketConfigProvider(config);

        assertThat(provider.size()).isEqualTo(2);

        S3BucketConfig cpConfig = provider.getBucketConfig("cp-bucket");
        assertThat(cpConfig.getPathStyleAccess()).isTrue();
        assertThat(cpConfig.getEndpoint()).isEqualTo("https://s3-compat.internal:9000");
        assertThat(cpConfig.getAccessKey()).isNull();
        assertThat(cpConfig.getRegion()).isNull();

        S3BucketConfig spConfig = provider.getBucketConfig("sp-bucket");
        assertThat(spConfig.getAccessKey()).isEqualTo("SP_ACCESS_KEY");
        assertThat(spConfig.getSecretKey()).isEqualTo("SP_SECRET_KEY");
        assertThat(spConfig.getRegion()).isEqualTo("eu-west-1");
        assertThat(spConfig.getAssumeRoleArn()).isEqualTo("arn:aws:iam::999:role/SavepointRole");
        assertThat(spConfig.getPathStyleAccess()).isNull();
    }
}
