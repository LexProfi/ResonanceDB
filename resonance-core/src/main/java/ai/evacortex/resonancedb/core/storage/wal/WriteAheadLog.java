/*
 * ResonanceDB — Waveform Semantic Engine
 * Copyright © 2025-2026 Aleksandr Listopad
 * SPDX-License-Identifier: LicenseRef-ResonanceDB-License-v1.0
 *
 * Patent notice: The authors intend to seek patent protection for this software.
 * Commercial use >30 days → license@evacortex.ai
 */
package ai.evacortex.resonancedb.core.storage.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class WriteAheadLog implements Closeable {

    static final byte[] MAGIC = { 'R', 'W', 'A', 'L' };
    static final short VERSION = 1;
    static final int FILE_HEADER_SIZE = 8;

    private final Path walDir;
    private final DurabilityMode durability;
    private final int groupSize;
    private final long groupIntervalNanos;
    private final long segmentMaxBytes;

    private final AtomicLong lsnCounter;
    private final AtomicLong epoch;

    private FileChannel currentChannel;
    private Path currentPath;
    private long currentFileSize;
    private int currentSeq;
    private long currentMaxLsn;

    private final LinkedBlockingQueue<PendingWrite> writeQueue = new LinkedBlockingQueue<>();
    private final Thread writerThread;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public enum DurabilityMode {
        STRICT, GROUP, ASYNC
    }

    record PendingWrite(byte[] data, long lsn, CompletableFuture<Long> future) {}

    public WriteAheadLog(Path walDir) throws IOException {
        this(walDir, DurabilityMode.GROUP, 256, 2, 128L << 20);
    }

    public WriteAheadLog(Path walDir, DurabilityMode durability,
                         int groupSize, long groupIntervalMs,
                         long segmentMaxBytes) throws IOException {
        this.walDir = walDir;
        this.durability = durability;
        this.groupSize = Math.max(1, groupSize);
        this.groupIntervalNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(1, groupIntervalMs));
        this.segmentMaxBytes = segmentMaxBytes;

        Files.createDirectories(walDir);

        List<WalFileInfo> existing = listWalFiles();
        if (existing.isEmpty()) {
            this.epoch = new AtomicLong(System.currentTimeMillis());
            this.lsnCounter = new AtomicLong(0);
            this.currentSeq = 0;
        } else {
            WalFileInfo last = existing.getLast();
            this.epoch = new AtomicLong(last.epoch);
            this.currentSeq = last.seq + 1;
            long maxLsn = scanMaxLsn(existing);
            this.lsnCounter = new AtomicLong(maxLsn);
        }

        openNewSegment();

        this.writerThread = new Thread(this::writerLoop, "wal-writer");
        this.writerThread.setDaemon(true);
        this.writerThread.start();
    }

    public CompletableFuture<Long> append(WalRecord record) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("WAL is closed"));
        }
        byte[] data = record.toBytes();
        CompletableFuture<Long> future = new CompletableFuture<>();
        PendingWrite pw = new PendingWrite(data, record.lsn(), future);
        writeQueue.add(pw);

        if (closed.get() && writeQueue.remove(pw)) {
            future.completeExceptionally(new IllegalStateException("WAL closed during append"));
        }
        return future;
    }

    public long nextLsn() {
        return lsnCounter.incrementAndGet();
    }

    public long currentLsn() {
        return lsnCounter.get();
    }

    public void checkpoint(long maxSealedLsn) throws IOException {
        long lsn = nextLsn();
        WalRecord cpRecord = WalRecord.checkpoint(lsn, maxSealedLsn);
        CompletableFuture<Long> f = append(cpRecord);
        f.join();

        truncate(maxSealedLsn);
    }

    public void truncate(long belowLsn) throws IOException {
        List<WalFileInfo> files = listWalFiles();
        for (WalFileInfo info : files) {
            if (info.path.equals(currentPath)) continue;
            long fileMaxLsn = scanFileMaxLsn(info.path);
            if (fileMaxLsn <= belowLsn) {
                Files.deleteIfExists(info.path);
            }
        }
    }

    public static List<WalRecord> replay(Path walDir) throws IOException, WalCorruptionException {
        if (!Files.isDirectory(walDir)) {
            return List.of();
        }

        List<WalFileInfo> files = listWalFilesStatic(walDir);
        if (files.isEmpty()) {
            return List.of();
        }

        List<WalRecord> records = new ArrayList<>();
        for (int fileIdx = 0; fileIdx < files.size(); fileIdx++) {
            boolean isLastFile = (fileIdx == files.size() - 1);
            Path path = files.get(fileIdx).path;

            byte[] fileData = Files.readAllBytes(path);
            if (fileData.length < FILE_HEADER_SIZE) {
                if (isLastFile) {
                    System.err.println("WARN WAL: truncated file header in " + path.getFileName());
                    continue;
                }
                throw new WalCorruptionException(
                        "WAL file too short for header: " + path.getFileName());
            }

            for (int i = 0; i < MAGIC.length; i++) {
                if (fileData[i] != MAGIC[i]) {
                    throw new WalCorruptionException(
                            "Invalid WAL magic in " + path.getFileName());
                }
            }

            ByteBuffer buf = ByteBuffer.wrap(fileData).order(ByteOrder.LITTLE_ENDIAN);
            buf.position(FILE_HEADER_SIZE);

            while (buf.hasRemaining()) {
                int posBeforeRecord = buf.position();
                try {
                    WalRecord record = WalRecord.fromBuffer(buf);
                    if (record == null) {
                        if (isLastFile) {
                            System.err.println("WARN WAL: discarding " +
                                    (buf.limit() - posBeforeRecord) +
                                    " bytes of partial tail in " + path.getFileName());
                            break;
                        } else {
                            throw new WalCorruptionException(
                                    "Partial record in non-tail WAL file " +
                                    path.getFileName() + " at position " + posBeforeRecord);
                        }
                    }
                    records.add(record);
                } catch (WalCorruptionException e) {
                    if (isLastFile) {
                        if (!hasValidRecordAfter(buf, posBeforeRecord)) {
                            System.err.println("WARN WAL: " + e.getMessage() +
                                    " — discarding tail of " + path.getFileName());
                            break;
                        }
                    }
                    throw new WalCorruptionException(
                            "Non-tail corruption in WAL file " + path.getFileName() +
                            ": " + e.getMessage(), e);
                }
            }
        }

        records.sort(Comparator.comparingLong(WalRecord::lsn));
        return records;
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) return;

        writerThread.interrupt();
        try {
            writerThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        drainQueueSync();

        if (currentChannel != null && currentChannel.isOpen()) {
            currentChannel.force(true);
            currentChannel.close();
        }
    }

    private void writerLoop() {
        List<PendingWrite> batch = new ArrayList<>(groupSize);

        while (!closed.get() || !writeQueue.isEmpty()) {
            batch.clear();

            try {
                PendingWrite first = writeQueue.poll(50, TimeUnit.MILLISECONDS);
                if (first == null) {
                    if (durability == DurabilityMode.ASYNC && currentChannel != null) {
                        try { currentChannel.force(true); } catch (IOException ignored) {}
                    }
                    continue;
                }
                batch.add(first);

                if (durability == DurabilityMode.STRICT) {
                    writeBatch(batch);
                    continue;
                }

                long deadlineNanos = System.nanoTime() + groupIntervalNanos;
                while (batch.size() < groupSize) {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0) break;
                    PendingWrite next = writeQueue.poll(
                            remaining, TimeUnit.NANOSECONDS);
                    if (next == null) break;
                    batch.add(next);
                }

                writeBatch(batch);

            } catch (InterruptedException e) {
                if (closed.get()) {
                    writeQueue.drainTo(batch);
                    if (!batch.isEmpty()) {
                        try { writeBatch(batch); } catch (Exception ignored) {}
                    }
                    return;
                }
            } catch (Exception e) {
                for (PendingWrite pw : batch) {
                    pw.future.completeExceptionally(e);
                }
            }
        }
    }

    private void writeBatch(List<PendingWrite> batch) throws IOException {
        if (batch.isEmpty()) return;

        for (PendingWrite pw : batch) {
            if (currentFileSize + pw.data.length > segmentMaxBytes) {
                currentChannel.force(true);
                FileChannel oldChannel = currentChannel;
                openNewSegment();
                oldChannel.close();
            }

            ByteBuffer dataBuf = ByteBuffer.wrap(pw.data);
            while (dataBuf.hasRemaining()) {
                currentChannel.write(dataBuf);
            }
            currentFileSize += pw.data.length;
            currentMaxLsn = Math.max(currentMaxLsn, pw.lsn);
        }

        boolean needSync = switch (durability) {
            case STRICT -> true;
            case GROUP -> true;
            case ASYNC -> false;
        };

        if (needSync) {
            currentChannel.force(true);
        }

        for (PendingWrite pw : batch) {
            pw.future.complete(pw.lsn);
        }
    }

    private void drainQueueSync() {
        List<PendingWrite> remaining = new ArrayList<>();
        writeQueue.drainTo(remaining);
        if (!remaining.isEmpty()) {
            try {
                writeBatch(remaining);
            } catch (IOException e) {
                for (PendingWrite pw : remaining) {
                    pw.future.completeExceptionally(e);
                }
            }
        }
    }

    private void openNewSegment() throws IOException {
        long ep = epoch.get();
        String fileName = String.format("%d-%04d.wal", ep, currentSeq);
        currentPath = walDir.resolve(fileName);
        currentChannel = FileChannel.open(currentPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        ByteBuffer header = ByteBuffer.allocate(FILE_HEADER_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN);
        header.put(MAGIC);
        header.putShort(VERSION);
        header.putShort((short) 0);
        header.flip();
        currentChannel.write(header);
        currentChannel.force(true);

        currentFileSize = FILE_HEADER_SIZE;
        currentMaxLsn = 0;
        currentSeq++;
    }

    record WalFileInfo(Path path, long epoch, int seq) {}

    List<WalFileInfo> listWalFiles() throws IOException {
        return listWalFilesStatic(walDir);
    }

    static List<WalFileInfo> listWalFilesStatic(Path walDir) throws IOException {
        List<WalFileInfo> result = new ArrayList<>();
        if (!Files.isDirectory(walDir)) return result;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(walDir, "*.wal")) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                String base = name.substring(0, name.length() - 4);
                int dash = base.indexOf('-');
                if (dash <= 0) continue;
                try {
                    long ep = Long.parseLong(base.substring(0, dash));
                    int seq = Integer.parseInt(base.substring(dash + 1));
                    result.add(new WalFileInfo(p, ep, seq));
                } catch (NumberFormatException ignored) {}
            }
        }
        result.sort(Comparator.comparingLong((WalFileInfo f) -> f.epoch)
                .thenComparingInt(f -> f.seq));
        return result;
    }

    private long scanMaxLsn(List<WalFileInfo> files) {
        long maxLsn = 0;
        for (WalFileInfo info : files) {
            try {
                long fileLsn = scanFileMaxLsn(info.path);
                maxLsn = Math.max(maxLsn, fileLsn);
            } catch (Exception e) {
                System.err.println("WAL scanMaxLsn: " + info.path.getFileName() + ": " + e.getMessage());
            }
        }
        return maxLsn;
    }

    private long scanFileMaxLsn(Path path) throws IOException {
        byte[] data = Files.readAllBytes(path);
        if (data.length < FILE_HEADER_SIZE) return 0;

        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        buf.position(FILE_HEADER_SIZE);

        long maxLsn = 0;
        while (buf.hasRemaining()) {
            try {
                WalRecord rec = WalRecord.fromBuffer(buf);
                if (rec == null) break;
                maxLsn = Math.max(maxLsn, rec.lsn());
            } catch (WalCorruptionException e) {
                break;
            }
        }
        return maxLsn;
    }

    private static boolean hasValidRecordAfter(ByteBuffer buf, int corruptionPos) {
        int savedPos = buf.position();
        try {
            int probeStart = corruptionPos + WalRecord.RECORD_OVERHEAD;
            for (int pos = probeStart; pos < buf.limit() - WalRecord.RECORD_OVERHEAD; pos++) {
                buf.position(pos);
                try {
                    WalRecord rec = WalRecord.fromBuffer(buf);
                    if (rec != null) return true;
                } catch (WalCorruptionException ignored) {
                }
            }
            return false;
        } finally {
            buf.position(savedPos);
        }
    }
}
