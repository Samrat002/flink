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
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalBlockLocation;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

/** Minimal S3 filesystem using AWS SDK v2. Not production-ready. */
class S3NativeFileSystem extends FileSystem {

    private final URI fsUri;
    private final S3Client s3;
    private final int readChunkBytes;
    private final long multipartThresholdBytes;
    private final long multipartPartSizeBytes;

    S3NativeFileSystem(@Nullable Configuration config, URI fsUri, S3Client s3Client) {
        this.fsUri = fsUri;
        this.s3 = s3Client;
        Configuration c = config == null ? new Configuration() : config;
        this.readChunkBytes = (int) c.get(NativeS3Options.READ_CHUNK_SIZE).getBytes();
        this.multipartThresholdBytes = c.get(NativeS3Options.MULTIPART_THRESHOLD).getBytes();
        this.multipartPartSizeBytes = c.get(NativeS3Options.MULTIPART_PART_SIZE).getBytes();
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(fsUri);
    }

    @Override
    public Path getHomeDirectory() {
        return new Path(fsUri);
    }

    @Override
    public boolean isDistributedFS() {
        return true;
    }

    @Override
    public URI getUri() {
        return fsUri;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        long fileLen = file.getLen();
        if (start < 0 || len < 0 || start > fileLen) {
            return new BlockLocation[0];
        }
        long remaining = Math.max(0L, Math.min(len, fileLen - start));
        return new BlockLocation[] {new LocalBlockLocation(remaining)};
    }

    private static String bucket(Path path) {
        String auth = path.toUri().getAuthority();
        if (auth == null) {
            throw new IllegalArgumentException("s3 path requires bucket: " + path);
        }
        return auth;
    }

    private static String key(Path path) {
        String p = path.toUri().getPath();
        if (p.startsWith("/")) {
            p = p.substring(1);
        }
        return p;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        final String b = bucket(f);
        final String k = key(f);
        HeadObjectResponse head =
                s3.headObject(HeadObjectRequest.builder().bucket(b).key(k).build());
        final long length = head.contentLength();

        return new FSDataInputStream() {
            long pos = 0;

            @Override
            public void seek(long desired) throws IOException {
                if (desired < 0 || desired > length) {
                    throw new IOException("Invalid seek");
                }
                pos = desired;
            }

            @Override
            public long getPos() {
                return pos;
            }

            @Override
            public int read(byte[] bbuf, int off, int len) throws IOException {
                if (pos >= length) {
                    return -1;
                }
                int toRead = (int) Math.min(len, Math.min(readChunkBytes, length - pos));
                String range = "bytes=" + pos + "-" + (pos + toRead - 1);
                GetObjectRequest req =
                        GetObjectRequest.builder().bucket(b).key(k).range(range).build();
                ResponseBytes<GetObjectResponse> bytes = s3.getObjectAsBytes(req);
                byte[] data = bytes.asByteArray();
                System.arraycopy(data, 0, bbuf, off, data.length);
                pos += data.length;
                return data.length;
            }

            @Override
            public int read() throws IOException {
                byte[] one = new byte[1];
                int r = read(one, 0, 1);
                return r == -1 ? -1 : (one[0] & 0xff);
            }
        };
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return open(f, 0);
    }

    @Override
    public FSDataOutputStream create(Path f, WriteMode overwrite) throws IOException {
        final String b = bucket(f);
        final String k = key(f);
        return new FSDataOutputStream() {
            private final java.io.ByteArrayOutputStream buffer =
                    new java.io.ByteArrayOutputStream();
            private String uploadId;
            private long uploaded = 0L;
            private final java.util.List<CompletedPart> completed = new java.util.ArrayList<>();
            private int partNumber = 1;

            @Override
            public long getPos() {
                return uploaded + buffer.size();
            }

            @Override
            public void flush() throws IOException {
                // no-op: we only flush parts on threshold; data remains buffered until then
            }

            @Override
            public void sync() throws IOException {
                // Best-effort: if in multipart mode and buffer has a full part, flush one
                maybeFlushPart();
            }

            @Override
            public void write(int bte) throws IOException {
                buffer.write(bte);
                maybeFlushPart();
            }

            @Override
            public void write(byte[] bbuf, int off, int len) throws IOException {
                buffer.write(bbuf, off, len);
                maybeFlushPart();
            }

            private void ensureMultipart() {
                if (uploadId == null) {
                    CreateMultipartUploadResponse create =
                            s3.createMultipartUpload(
                                    CreateMultipartUploadRequest.builder()
                                            .bucket(b)
                                            .key(k)
                                            .build());
                    uploadId = create.uploadId();
                }
            }

            private void maybeFlushPart() {
                if (uploadId == null && buffer.size() < multipartThresholdBytes) {
                    return;
                }
                if (uploadId == null) {
                    ensureMultipart();
                }
                if (buffer.size() >= multipartPartSizeBytes) {
                    byte[] bytes = buffer.toByteArray();
                    buffer.reset();
                    UploadPartResponse upr =
                            s3.uploadPart(
                                    UploadPartRequest.builder()
                                            .bucket(b)
                                            .key(k)
                                            .partNumber(partNumber)
                                            .uploadId(uploadId)
                                            .build(),
                                    RequestBody.fromBytes(bytes));
                    completed.add(
                            CompletedPart.builder()
                                    .partNumber(partNumber)
                                    .eTag(upr.eTag())
                                    .build());
                    uploaded += bytes.length;
                    partNumber++;
                }
            }

            @Override
            public void close() throws IOException {
                if (uploadId == null) {
                    PutObjectRequest put = PutObjectRequest.builder().bucket(b).key(k).build();
                    s3.putObject(put, RequestBody.fromBytes(buffer.toByteArray()));
                    return;
                }
                if (buffer.size() > 0) {
                    byte[] bytes = buffer.toByteArray();
                    buffer.reset();
                    UploadPartResponse upr =
                            s3.uploadPart(
                                    UploadPartRequest.builder()
                                            .bucket(b)
                                            .key(k)
                                            .partNumber(partNumber)
                                            .uploadId(uploadId)
                                            .build(),
                                    RequestBody.fromBytes(bytes));
                    completed.add(
                            CompletedPart.builder()
                                    .partNumber(partNumber)
                                    .eTag(upr.eTag())
                                    .build());
                }
                s3.completeMultipartUpload(
                        CompleteMultipartUploadRequest.builder()
                                .bucket(b)
                                .key(k)
                                .multipartUpload(
                                        CompletedMultipartUpload.builder().parts(completed).build())
                                .uploadId(uploadId)
                                .build());
            }
        };
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        try {
            s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket(f)).key(key(f)).build());
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw new IOException(e);
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        try {
            HeadObjectResponse head =
                    s3.headObject(
                            HeadObjectRequest.builder().bucket(bucket(f)).key(key(f)).build());
            return new SimpleFileStatus(head.contentLength(), false, 1, 0, 0, f);
        } catch (S3Exception e) {
            // treat as directory if list returns children
            ListObjectsV2Response listed =
                    s3.listObjectsV2(
                            ListObjectsV2Request.builder()
                                    .bucket(bucket(f))
                                    .prefix(key(f).endsWith("/") ? key(f) : key(f) + "/")
                                    .maxKeys(1)
                                    .build());
            if (!listed.contents().isEmpty()) {
                return new SimpleFileStatus(0, true, 1, 0, 0, f);
            }
            throw new IOException("Not found: " + f, e);
        }
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        final String b = bucket(f);
        final String prefix = key(f);
        String token = null;
        java.util.ArrayList<FileStatus> results = new java.util.ArrayList<>();
        do {
            ListObjectsV2Response listed =
                    s3.listObjectsV2(
                            ListObjectsV2Request.builder()
                                    .bucket(b)
                                    .prefix(prefix)
                                    .delimiter("/")
                                    .continuationToken(token)
                                    .build());
            for (S3Object o : listed.contents()) {
                results.add(
                        new SimpleFileStatus(
                                o.size(), false, 1, 0, 0, new Path("s3://" + b + "/" + o.key())));
            }
            token = listed.isTruncated() ? listed.nextContinuationToken() : null;
        } while (token != null);
        return results.toArray(new FileStatus[0]);
    }

    @Override
    public boolean mkdirs(Path f) {
        // S3 is object store; simulate directory by creating zero-byte marker
        PutObjectRequest put =
                PutObjectRequest.builder()
                        .bucket(bucket(f))
                        .key(key(f).endsWith("/") ? key(f) : key(f) + "/")
                        .build();
        s3.putObject(put, RequestBody.fromByteBuffer(ByteBuffer.allocate(0)));
        return true;
    }

    // Helper for recursive prefix deletion
    public boolean delete(Path f, boolean recursive, boolean allowNonEmptyDirectory)
            throws IOException {
        if (!recursive) {
            return delete(f, false);
        }
        final String b = bucket(f);
        final String prefix = key(f).endsWith("/") ? key(f) : key(f) + "/";
        String token = null;
        do {
            ListObjectsV2Response listed =
                    s3.listObjectsV2(
                            ListObjectsV2Request.builder()
                                    .bucket(b)
                                    .prefix(prefix)
                                    .continuationToken(token)
                                    .build());
            if (!listed.contents().isEmpty()) {
                java.util.List<ObjectIdentifier> ids = new java.util.ArrayList<>();
                for (S3Object o : listed.contents()) {
                    ids.add(ObjectIdentifier.builder().key(o.key()).build());
                }
                s3.deleteObjects(
                        DeleteObjectsRequest.builder()
                                .bucket(b)
                                .delete(Delete.builder().objects(ids).build())
                                .build());
            }
            token = listed.isTruncated() ? listed.nextContinuationToken() : null;
        } while (token != null);
        return true;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        // Implement rename as copy + delete, consistent with object store semantics
        final String sb = bucket(src);
        final String sk = key(src);
        final String db = bucket(dst);
        final String dk = key(dst);

        // Perform copy
        s3.copyObject(
                c -> c.sourceBucket(sb).sourceKey(sk).destinationBucket(db).destinationKey(dk));
        // Delete source
        s3.deleteObject(DeleteObjectRequest.builder().bucket(sb).key(sk).build());
        return true;
    }
}
