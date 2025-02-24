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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions.CompressionCodec;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.UnknownTierShuffleDescriptor;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleUtils;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloatConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.consumer.InputGateSpecUtils.createGateBuffersSpec;
import static org.apache.flink.runtime.shuffle.ShuffleUtils.applyWithShuffleTypeCheck;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Factory for {@link SingleInputGate} to use in {@link NettyShuffleEnvironment}. */
public class SingleInputGateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SingleInputGateFactory.class);

    @Nonnull protected final ResourceID taskExecutorResourceId;

    protected final int partitionRequestInitialBackoff;

    protected final int partitionRequestMaxBackoff;

    protected final int partitionRequestListenerTimeout;

    @Nonnull protected final ConnectionManager connectionManager;

    @Nonnull protected final ResultPartitionManager partitionManager;

    @Nonnull protected final TaskEventPublisher taskEventPublisher;

    @Nonnull protected final NetworkBufferPool networkBufferPool;

    private final Optional<Integer> maxRequiredBuffersPerGate;

    protected final int configuredNetworkBuffersPerChannel;

    private final int floatingNetworkBuffersPerGate;

    private final boolean batchShuffleCompressionEnabled;

    private final CompressionCodec compressionCodec;

    private final int networkBufferSize;

    private final BufferDebloatConfiguration debloatConfiguration;

    /** The following attributes will be null if tiered storage shuffle is disabled. */
    @Nullable private final TieredStorageConfiguration tieredStorageConfiguration;

    @Nullable private final TieredStorageNettyServiceImpl tieredStorageNettyService;

    public SingleInputGateFactory(
            @Nonnull ResourceID taskExecutorResourceId,
            @Nonnull NettyShuffleEnvironmentConfiguration networkConfig,
            @Nonnull ConnectionManager connectionManager,
            @Nonnull ResultPartitionManager partitionManager,
            @Nonnull TaskEventPublisher taskEventPublisher,
            @Nonnull NetworkBufferPool networkBufferPool,
            @Nullable TieredStorageConfiguration tieredStorageConfiguration,
            @Nullable TieredStorageNettyServiceImpl tieredStorageNettyService) {
        this.taskExecutorResourceId = taskExecutorResourceId;
        this.partitionRequestInitialBackoff = networkConfig.partitionRequestInitialBackoff();
        this.partitionRequestMaxBackoff = networkConfig.partitionRequestMaxBackoff();
        this.partitionRequestListenerTimeout = networkConfig.getPartitionRequestListenerTimeout();
        this.maxRequiredBuffersPerGate = networkConfig.maxRequiredBuffersPerGate();
        this.configuredNetworkBuffersPerChannel =
                NettyShuffleUtils.getNetworkBuffersPerInputChannel(
                        networkConfig.networkBuffersPerChannel());
        this.floatingNetworkBuffersPerGate = networkConfig.floatingNetworkBuffersPerGate();
        this.batchShuffleCompressionEnabled = networkConfig.isBatchShuffleCompressionEnabled();
        this.compressionCodec = networkConfig.getCompressionCodec();
        this.networkBufferSize = networkConfig.networkBufferSize();
        this.connectionManager = connectionManager;
        this.partitionManager = partitionManager;
        this.taskEventPublisher = taskEventPublisher;
        this.networkBufferPool = networkBufferPool;
        this.debloatConfiguration = networkConfig.getDebloatConfiguration();
        this.tieredStorageConfiguration = tieredStorageConfiguration;
        this.tieredStorageNettyService = tieredStorageNettyService;
    }

    /** Creates an input gate and all of its input channels. */
    public SingleInputGate create(
            @Nonnull ShuffleIOOwnerContext owner,
            int gateIndex,
            @Nonnull InputGateDeploymentDescriptor igdd,
            @Nonnull PartitionProducerStateProvider partitionProducerStateProvider,
            @Nonnull InputChannelMetrics metrics) {
        GateBuffersSpec gateBuffersSpec =
                createGateBuffersSpec(
                        maxRequiredBuffersPerGate,
                        configuredNetworkBuffersPerChannel,
                        floatingNetworkBuffersPerGate,
                        igdd.getConsumedPartitionType(),
                        igdd.getNumConsumedShuffleDescriptors(),
                        tieredStorageConfiguration != null);
        SupplierWithException<BufferPool, IOException> bufferPoolFactory =
                createBufferPoolFactory(
                        networkBufferPool,
                        gateBuffersSpec.getRequiredFloatingBuffers(),
                        gateBuffersSpec.getTotalFloatingBuffers());

        BufferDecompressor bufferDecompressor = null;
        if (igdd.getConsumedPartitionType().supportCompression()
                && batchShuffleCompressionEnabled) {
            bufferDecompressor = new BufferDecompressor(networkBufferSize, compressionCodec);
        }

        final String owningTaskName = owner.getOwnerName();
        final MetricGroup networkInputGroup = owner.getInputGroup();

        SingleInputGate inputGate =
                new SingleInputGate(
                        owningTaskName,
                        gateIndex,
                        igdd.getConsumedResultId(),
                        igdd.getConsumedPartitionType(),
                        igdd.getNumConsumedShuffleDescriptors(),
                        partitionProducerStateProvider,
                        bufferPoolFactory,
                        bufferDecompressor,
                        networkBufferPool,
                        networkBufferSize,
                        new ThroughputCalculator(SystemClock.getInstance()),
                        maybeCreateBufferDebloater(
                                owningTaskName, gateIndex, networkInputGroup.addGroup(gateIndex)));

        createInputChannelsAndTieredStorageService(
                owningTaskName, igdd, inputGate, gateBuffersSpec, metrics);
        return inputGate;
    }

    private BufferDebloater maybeCreateBufferDebloater(
            String owningTaskName, int gateIndex, MetricGroup inputGroup) {
        if (debloatConfiguration.isEnabled()) {
            final BufferDebloater bufferDebloater =
                    new BufferDebloater(
                            owningTaskName,
                            gateIndex,
                            debloatConfiguration.getTargetTotalTime().toMillis(),
                            debloatConfiguration.getStartingBufferSize(),
                            debloatConfiguration.getMaxBufferSize(),
                            debloatConfiguration.getMinBufferSize(),
                            debloatConfiguration.getBufferDebloatThresholdPercentages(),
                            debloatConfiguration.getNumberOfSamples());
            inputGroup.gauge(
                    MetricNames.ESTIMATED_TIME_TO_CONSUME_BUFFERS,
                    () -> bufferDebloater.getLastEstimatedTimeToConsumeBuffers().toMillis());
            inputGroup.gauge(MetricNames.DEBLOATED_BUFFER_SIZE, bufferDebloater::getLastBufferSize);
            return bufferDebloater;
        }

        return null;
    }

    private void createInputChannelsAndTieredStorageService(
            String owningTaskName,
            InputGateDeploymentDescriptor inputGateDeploymentDescriptor,
            SingleInputGate inputGate,
            GateBuffersSpec gateBuffersSpec,
            InputChannelMetrics metrics) {
        ShuffleDescriptor[] shuffleDescriptors =
                inputGateDeploymentDescriptor.getShuffleDescriptors();

        // Create the input channels. There is one input channel for each consumed subpartition.
        int inputChannelSize = inputGateDeploymentDescriptor.getNumConsumedShuffleDescriptors();
        InputChannel[] inputChannels = new InputChannel[inputChannelSize];

        ChannelStatistics channelStatistics = new ChannelStatistics();

        int channelIdx = 0;
        final List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs = new ArrayList<>();
        List<List<TierShuffleDescriptor>> tierShuffleDescriptors = new ArrayList<>();

        for (IndexRange consumedShuffleDescriptorRange :
                inputGateDeploymentDescriptor.getConsumedShuffleDescriptorRanges()) {
            for (int i = consumedShuffleDescriptorRange.getStartIndex();
                    i <= consumedShuffleDescriptorRange.getEndIndex();
                    i++) {
                ResultSubpartitionIndexSet subpartitionIndexSet =
                        new ResultSubpartitionIndexSet(
                                inputGateDeploymentDescriptor.getConsumedSubpartitionRange(i));
                ShuffleDescriptor descriptor = shuffleDescriptors[i];
                TieredStoragePartitionId partitionId =
                        TieredStorageIdMappingUtils.convertId(descriptor.getResultPartitionID());
                inputChannels[channelIdx] =
                        createInputChannel(
                                inputGate,
                                channelIdx,
                                gateBuffersSpec.getEffectiveExclusiveBuffersPerChannel(),
                                descriptor,
                                subpartitionIndexSet,
                                channelStatistics,
                                metrics);
                if (tieredStorageConfiguration != null) {
                    addTierShuffleDescriptors(tierShuffleDescriptors, descriptor);

                    tieredStorageConsumerSpecs.add(
                            new TieredStorageConsumerSpec(
                                    inputGate.getInputGateIndex(),
                                    partitionId,
                                    new TieredStorageInputChannelId(channelIdx),
                                    subpartitionIndexSet));
                }
                channelIdx++;
            }
        }

        checkState(channelIdx == inputChannelSize);

        inputGate.setInputChannels(inputChannels);

        if (tieredStorageConfiguration != null) {
            TieredStorageConsumerClient tieredStorageConsumerClient =
                    new TieredStorageConsumerClient(
                            tieredStorageConfiguration.getTierFactories(),
                            tieredStorageConsumerSpecs,
                            tierShuffleDescriptors,
                            tieredStorageNettyService);
            inputGate.setTieredStorageService(
                    tieredStorageConsumerSpecs,
                    tieredStorageConsumerClient,
                    tieredStorageNettyService);
        }

        LOG.debug(
                "{}: Created {} input channels ({}).",
                owningTaskName,
                inputChannels.length,
                channelStatistics);
    }

    private InputChannel createInputChannel(
            SingleInputGate inputGate,
            int index,
            int buffersPerChannel,
            ShuffleDescriptor shuffleDescriptor,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            ChannelStatistics channelStatistics,
            InputChannelMetrics metrics) {
        return applyWithShuffleTypeCheck(
                NettyShuffleDescriptor.class,
                shuffleDescriptor,
                unknownShuffleDescriptor -> {
                    channelStatistics.numUnknownChannels++;
                    return new UnknownInputChannel(
                            inputGate,
                            index,
                            unknownShuffleDescriptor.getResultPartitionID(),
                            subpartitionIndexSet,
                            partitionManager,
                            taskEventPublisher,
                            connectionManager,
                            partitionRequestInitialBackoff,
                            partitionRequestMaxBackoff,
                            partitionRequestListenerTimeout,
                            buffersPerChannel,
                            metrics);
                },
                nettyShuffleDescriptor ->
                        createKnownInputChannel(
                                inputGate,
                                index,
                                buffersPerChannel,
                                nettyShuffleDescriptor,
                                subpartitionIndexSet,
                                channelStatistics,
                                metrics));
    }

    @VisibleForTesting
    protected InputChannel createKnownInputChannel(
            SingleInputGate inputGate,
            int index,
            int buffersPerChannel,
            NettyShuffleDescriptor inputChannelDescriptor,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            ChannelStatistics channelStatistics,
            InputChannelMetrics metrics) {
        ResultPartitionID partitionId = inputChannelDescriptor.getResultPartitionID();
        if (inputChannelDescriptor.isLocalTo(taskExecutorResourceId)) {
            // Consuming task is deployed to the same TaskManager as the partition => local
            channelStatistics.numLocalChannels++;
            return new LocalRecoveredInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    subpartitionIndexSet,
                    partitionManager,
                    taskEventPublisher,
                    partitionRequestInitialBackoff,
                    partitionRequestMaxBackoff,
                    buffersPerChannel,
                    metrics);
        } else {
            // Different instances => remote
            channelStatistics.numRemoteChannels++;
            return new RemoteRecoveredInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    subpartitionIndexSet,
                    inputChannelDescriptor.getConnectionId(),
                    connectionManager,
                    partitionRequestInitialBackoff,
                    partitionRequestMaxBackoff,
                    partitionRequestListenerTimeout,
                    buffersPerChannel,
                    metrics);
        }
    }

    private void addTierShuffleDescriptors(
            List<List<TierShuffleDescriptor>> tierShuffleDescriptors,
            ShuffleDescriptor descriptor) {
        if (descriptor instanceof NettyShuffleDescriptor) {
            tierShuffleDescriptors.add(
                    ((NettyShuffleDescriptor) descriptor).getTierShuffleDescriptors());
        } else if (descriptor.isUnknown()) {
            List<TierShuffleDescriptor> unknownDescriptors = new ArrayList<>();
            int numTiers = checkNotNull(tieredStorageConfiguration).getTierFactories().size();
            for (int i = 0; i < numTiers; i++) {
                unknownDescriptors.add(UnknownTierShuffleDescriptor.INSTANCE);
            }
            tierShuffleDescriptors.add(unknownDescriptors);
        } else {
            throw new IllegalArgumentException("Unsupported shuffle descriptor type " + descriptor);
        }
    }

    @VisibleForTesting
    static SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
            BufferPoolFactory bufferPoolFactory,
            int minFloatingBuffersPerGate,
            int maxFloatingBuffersPerGate) {
        Pair<Integer, Integer> pair = Pair.of(minFloatingBuffersPerGate, maxFloatingBuffersPerGate);
        return () -> bufferPoolFactory.createBufferPool(pair.getLeft(), pair.getRight());
    }

    /** Statistics of input channels. */
    protected static class ChannelStatistics {
        int numLocalChannels;
        int numRemoteChannels;
        int numUnknownChannels;

        @Override
        public String toString() {
            return String.format(
                    "local: %s, remote: %s, unknown: %s",
                    numLocalChannels, numRemoteChannels, numUnknownChannels);
        }
    }
}
