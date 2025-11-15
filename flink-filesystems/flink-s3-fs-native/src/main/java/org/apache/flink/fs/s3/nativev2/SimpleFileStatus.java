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

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

final class SimpleFileStatus implements FileStatus {
    private final long len;
    private final boolean dir;
    private final short replication;
    private final long blockSize;
    private final long modificationTime;
    private final Path path;

    SimpleFileStatus(
            long len,
            boolean dir,
            int replication,
            long blockSize,
            long modificationTime,
            Path path) {
        this.len = len;
        this.dir = dir;
        this.replication = (short) replication;
        this.blockSize = blockSize;
        this.modificationTime = modificationTime;
        this.path = path;
    }

    @Override
    public long getLen() {
        return len;
    }

    @Override
    public long getBlockSize() {
        return blockSize;
    }

    @Override
    public short getReplication() {
        return replication;
    }

    @Override
    public long getModificationTime() {
        return modificationTime;
    }

    @Override
    public long getAccessTime() {
        return 0L;
    }

    @Override
    public boolean isDir() {
        return dir;
    }

    @Override
    public Path getPath() {
        return path;
    }
}
