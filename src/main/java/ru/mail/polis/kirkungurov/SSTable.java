package ru.mail.polis.kirkungurov;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;

public class SSTable implements Table {
//    private static final Logger LOGGER = Logger.getLogger(SSTable.class.getName());


    private final FileChannel fileChannel;
    private final long size;
    private final int count;

    public SSTable(@NotNull final File file) throws IOException {
        fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        final long fileSize = fileChannel.size() - Integer.BYTES;
        final ByteBuffer cellBuffer = ByteBuffer.allocate(Integer.BYTES);
        this.fileChannel.read(cellBuffer, fileSize);
        this.count = cellBuffer.rewind().getInt();
        this.size = fileSize - count * Integer.BYTES;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<Cell>() {

            int position = binarySearch(from);

            @Override
            public boolean hasNext() {
                return position < count;
            }

            @Override
            public Cell next() {
                try {
                    return getCell(position++);
                } catch (IOException e) {
                    return null;
                    //throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("Unavailable operation");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("Unavailable operation");
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }

    @Override
    public long size() {
        return size;
    }

    /**
     * keySize|key|Timestamp|tombstone|valueSize|value
     */
    public static void serialize(@NotNull final File file,
                                 @NotNull final Iterator<Cell> iterator) throws IOException {
        try (FileChannel fileWriter = FileChannel.open(
                file.toPath(),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            final ArrayList<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (iterator.hasNext()) {
                offsets.add(offset);
                final Cell tmp = iterator.next();
                final ByteBuffer key = tmp.getKey();
                final Value value = tmp.getValue();
                final int keySize = key.remaining();
                offset += keySize + Integer.BYTES + Long.BYTES;
                fileWriter.write(ByteBuffer.allocate(Integer.BYTES)
                        .putInt(keySize)
                        .rewind());
                fileWriter.write(key);

                if (value.isTombstone()) {
                    fileWriter.write(ByteBuffer.allocate(Long.BYTES)
                            .putLong(-value.getTimestamp())
                            .rewind());
                } else {
                    fileWriter.write(ByteBuffer.allocate(Long.BYTES)
                            .putLong(value.getTimestamp())
                            .rewind());

                    final ByteBuffer data = value.getData();
                    final int valueSize = data.remaining();
                    offset += valueSize;
                    fileWriter.write(ByteBuffer.allocate(Integer.BYTES)
                            .putInt(valueSize)
                            .rewind());
                    fileWriter.write(data);
                }
            }

            for (Integer tmpOffset : offsets) {
                fileWriter.write(ByteBuffer.allocate(Integer.BYTES)
                        .putInt(tmpOffset)
                        .rewind());
            }

            int count = offsets.size();
            fileWriter.write(ByteBuffer.allocate(Integer.BYTES)
                    .putInt(count)
                    .rewind());
        }
    }

    private int binarySearch(@NotNull final ByteBuffer from) throws IOException {
        int l = 0;
        int r = count - 1;

        while (l <= r) {
            int m = (l + r) / 2;
            int cmp = getKey(m).compareTo(from);
            if (cmp < 0) {
                l = m + 1;
            } else if (cmp > 0) {
                r = m - 1;
            } else {
                return m;
            }
        }
        return l;
    }

    private int getOffset(final int num) throws IOException {
        ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(tmp, size + num * Integer.BYTES);
        return tmp.rewind().getInt();
    }

    @NotNull
    private ByteBuffer getKey(final int row) throws IOException {
        final int offset = getOffset(row);
        final ByteBuffer keySize = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(keySize, offset);
        final ByteBuffer key = ByteBuffer.allocate(keySize.rewind().getInt());
        fileChannel.read(key, offset + Integer.BYTES);
        return key.rewind();
    }

    @NotNull
    private Cell getCell(final int row) throws IOException {
        int offset = getOffset(row);
        ByteBuffer key = getKey(row);

        offset += Integer.BYTES + key.remaining();
        final ByteBuffer timestamp = ByteBuffer.allocate(Long.BYTES);
        fileChannel.read(timestamp, offset);
        offset += Long.BYTES;

        if (timestamp.rewind().getLong() < 0) {
            return new Cell(key, new Value(-timestamp.rewind().getLong()));
        } else {
            ByteBuffer valueSize = ByteBuffer.allocate(Integer.BYTES);
            fileChannel.read(valueSize, offset);
            final ByteBuffer value = ByteBuffer.allocate(valueSize.rewind().getInt());
            offset += Integer.BYTES;
            fileChannel.read(value, offset);
            return new Cell(key, new Value(timestamp.rewind().getLong(), value.rewind()));
        }
    }
}
