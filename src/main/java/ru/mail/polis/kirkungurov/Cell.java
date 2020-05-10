package ru.mail.polis.kirkungurov;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class Cell implements Comparable<Cell> {

    @NotNull
    private final ByteBuffer key;
    @NotNull
    private final Value value;

    public Cell(@NotNull final ByteBuffer key, @NotNull final Value value) {
        this.key = key;
        this.value = value;
    }

    public Cell(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        this.key = key;
        this.value = new Value(value);
    }

    @NotNull
    public ByteBuffer getKey() {
        return this.key;
    }

    @NotNull
    public Value getValue() {
        return this.value;
    }

    @Override
    public int compareTo(@NotNull final Cell cell) {
        final int cmp = key.compareTo(cell.key);
        return cmp == 0 ? value.compareTo(cell.value) : cmp;
    }
}