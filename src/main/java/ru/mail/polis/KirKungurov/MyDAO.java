package ru.mail.polis.KirKungurov;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MyDAO implements DAO {
    private SortedMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) {
        return Iterators.transform(map
                        .tailMap(from)
                        .entrySet().iterator(),
                element -> Record.of(element.getKey(), element.getValue()));

    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        map.remove(key);
    }

    @Override
    public void close() {
        map.clear();
    }
}
