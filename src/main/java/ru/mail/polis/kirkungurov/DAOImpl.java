package ru.mail.polis.kirkungurov;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.logging.Logger;

public class DAOImpl implements DAO {
    private static final Logger LOGGER = Logger.getLogger(DAOImpl.class.getName());

    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    @NotNull
    private final File storage;
    private final long tableByteSyze;

    @NotNull
    private final MemTable memTable;
    @NotNull
    private final NavigableMap<Integer, Table> ssTables;

    private int generation;

    public DAOImpl(final File storage, final long tableByteSize) {
        this.storage = storage;
        this.tableByteSyze = tableByteSize;
        this.memTable = new MemTable();
        this.ssTables = new TreeMap<>();
        this.generation = -1;

        File[] list = storage.listFiles((dir1, name) -> name.endsWith(SUFFIX));
        assert list != null;
        LOGGER.info(String.valueOf(list.length) + " files found with suffix");
        Arrays.stream(list)
                .filter(curFile -> !curFile.isDirectory())
                .forEach(file -> {
                    String name = file.getName();
                    String nameWithoutSuf = name.substring(0, name.indexOf(SUFFIX));
                    if (nameWithoutSuf.matches("[0-9]+")) {
                        int version = Integer.parseInt(nameWithoutSuf);
                        generation = Math.max(generation, version);
                        try {
                            ssTables.put(version, new SSTable(file));
                            LOGGER.info("Put this file into sstable " + file);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                });
        generation++;
        LOGGER.info("SSTable size: " + ssTables.size());
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) {
        final List<Iterator<Cell>> iterators = new ArrayList<>(ssTables.size() + 1);
        LOGGER.info("Iterators count: " + iterators.size());
        LOGGER.info("ssTables size: " + ssTables.size());
        iterators.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(ssTable -> {
            try {
                iterators.add(ssTable.iterator(from));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        final Iterator<Cell> mergedElements = Iterators.mergeSorted(iterators,
                Comparator.comparing(Cell::getKey).thenComparing(Cell::getValue));
        final Iterator<Cell> freshElements = Iters.collapseEquals(mergedElements, Cell::getKey);
        final Iterator<Cell> archiveElements = Iterators.filter(freshElements,
                element -> !element.getValue().isTombstone());
        return Iterators.transform(archiveElements,
                element -> Record.of(element.getKey(), element.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.size() >= tableByteSyze) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.size() >= tableByteSyze) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() >= 0) {
            flush();
        }
        for (Table table : ssTables.values()) {
            table.close();
        }
    }

    private void flush() throws IOException {
        File file = new File(storage, generation + TEMP);
        SSTable.serialize(file, memTable.iterator(ByteBuffer.allocate(0)));
        File dst = new File(storage, generation + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        generation++;
        ssTables.put(generation, new SSTable(dst));
        memTable.close();
    }
}
