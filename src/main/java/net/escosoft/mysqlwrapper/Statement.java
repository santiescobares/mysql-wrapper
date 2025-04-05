package net.escosoft.mysqlwrapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import net.escosoft.mysqlwrapper.table.Table;
import net.escosoft.mysqlwrapper.table.TableColumn;
import net.escosoft.mysqlwrapper.util.Preconditions;
import net.escosoft.mysqlwrapper.util.StringUtil;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Statement {

    private static final String SET_FORMAT = "%s = ?";

    private final StringBuilder builder = new StringBuilder();

    private PreparedStatement preparedStatement;

    private boolean needsReplacements;
    private Object[] replacements;

    /**
     * Adds a whole raw statement string to the current PreparedStatement.
     *
     * @param statement the raw statement string.
     */
    public Statement of(String statement) {
        Preconditions.checkNonNull(statement, "Statement can't be null.");
        this.builder.append(statement);
        return this;
    }

    public Statement selectFrom(Table from, String... columns) {
        Preconditions.checkNonNull(from, "Table can't be null.");
        Preconditions.checkNonNull(columns, "Columns can't be null.");
        this.builder.append("SELECT ")
                .append(StringUtil.join(", ", Arrays.asList(columns)))
                .append(" FROM ")
                .append(from.getName());
        return this;
    }

    public Statement selectFrom(Table from, TableColumn... columns) {
        Preconditions.checkNonNull(from, "Table can't be null.");
        Preconditions.checkNonNull(columns, "Columns can't be null.");
        this.builder.append("SELECT ")
                .append(StringUtil.join(", ", Stream.of(columns)
                        .map(column -> "'" + column.getName() + "'")
                        .collect(Collectors.toList()))
                )
                .append(" FROM ")
                .append(from.getName());
        return this;
    }

    public Statement selectFrom(Table from, int amount) {
        Preconditions.checkNonNull(from, "Table can't be null.");
        this.builder.append("SELECT ")
                .append(amount == -1 ? "*" : amount)
                .append(" FROM ")
                .append(from.getName());
        return this;
    }

    public Statement selectAllFrom(Table from) {
        return this.selectFrom(from, -1);
    }

    public Statement insertInto(Table into) {
        Preconditions.checkNonNull(into, "Table can't be null.");
        this.builder.append("INSERT INTO ")
                .append(into.getName());
        return this;
    }

    public Statement deleteFrom(Table from) {
        Preconditions.checkNonNull(from, "Table can't be null.");
        this.builder.append("DELETE FROM ")
                .append(from.getName());
        return this;
    }

    public Statement update(Table table) {
        Preconditions.checkNonNull(table, "Table can't be null.");
        this.builder.append("UPDATE ")
                .append(table.getName());
        return this;
    }

    public Statement set(TableColumn column, String value) {
        Preconditions.checkNonNull(column, "Column can't be null.");
        Preconditions.checkNonNull(value, "Value can't be null.");
        this.builder.append(" SET ")
                .append(column.getName())
                .append(" = ")
                .append(value);
        return this;
    }

    public Statement set(TableColumn... columns) {
        Preconditions.checkNonNull(columns, "Columns can't be null.");
        this.needsReplacements = true;
        this.builder.append(" SET ")
                .append(StringUtil.join(", ", Stream.of(columns)
                        .map(column -> String.format(SET_FORMAT, column.getName()))
                        .collect(Collectors.toList()))
                );
        return this;
    }

    public Statement values(String valuesString) {
        Preconditions.checkNonNull(valuesString, "Values can't be null.");
        this.builder.append(" VALUE ")
                .append(valuesString);
        return this;
    }

    public Statement values(int amount) {
        this.needsReplacements = true;
        return this.values("(" + StringUtil.join(",", Collections.nCopies(amount, "?")) + ")");
    }

    public Statement replacements(Object... replacements) {
        Preconditions.checkNonNull(replacements, "Replacements can't be null.");
        this.replacements = replacements;
        return this;
    }

    public Statement where() {
        this.builder.append(" WHERE");
        return this;
    }

    public Statement equals(String column, String value) {
        Preconditions.checkNonNull(column, "Column can't be null.");
        Preconditions.checkNonNull(value, "Value can't be null.");
        this.builder.append(" ")
                .append(column)
                .append(" = ")
                .append(value);
        return this;
    }

    public Statement equals(TableColumn column, String value) {
        return this.equals(column.getName(), value);
    }

    public Statement equals(String column) {
        this.needsReplacements = true;
        return this.equals(column, "?");
    }

    public Statement equals(TableColumn column) {
        return this.equals(column.getName());
    }

    public Statement lowerThan(String column, String value) {
        Preconditions.checkNonNull(column, "Column can't be null.");
        Preconditions.checkNonNull(value, "Value can't be null.");
        this.builder.append(" ")
                .append(column)
                .append(" < ")
                .append(value);
        return this;
    }

    public Statement lowerThan(TableColumn column, String value) {
        return this.lowerThan(column.getName(), value);
    }

    public Statement lowerThan(String column) {
        this.needsReplacements = true;
        return this.lowerThan(column, "?");
    }

    public Statement lowerThan(TableColumn column) {
        return this.lowerThan(column.getName());
    }

    public Statement greaterThan(String column, String value) {
        Preconditions.checkNonNull(column, "Column can't be null.");
        Preconditions.checkNonNull(value, "Value can't be null.");
        this.builder.append(" ")
                .append(column)
                .append(" > ")
                .append(value);
        return this;
    }

    public Statement greaterThan(TableColumn column, String value) {
        return this.greaterThan(column.getName(), value);
    }

    public Statement greaterThan(String column) {
        this.needsReplacements = true;
        return this.lowerThan(column, "?");
    }

    public Statement greaterThan(TableColumn column) {
        return this.greaterThan(column.getName());
    }

    public Statement in(String column, String values) {
        Preconditions.checkNonNull(column, "Column can't be null.");
        Preconditions.checkNonNull(values, "Values can't be null.");
        this.builder.append(" ")
                .append(column)
                .append(" IN ")
                .append(values);
        return this;
    }

    public Statement in(TableColumn column, String values) {
        return this.in(column.getName(), values);
    }

    public Statement and() {
        this.builder.append(" AND");
        return this;
    }

    public Statement or() {
        this.builder.append(" OR");
        return this;
    }

    public Statement as(String label) {
        Preconditions.checkNonNull(label, "Label can't be null.");
        this.builder.append(" AS ")
                .append(label);
        return this;
    }

    public Statement on() {
        this.builder.append(" ON");
        return this;
    }

    public Statement join(Table table) {
        Preconditions.checkNonNull(table, "Table can't be null.");
        this.builder.append(" JOIN ")
                .append(table.getName());
        return this;
    }

    public int executeUpdate() {
        try (Connection connection = Database.getConnection()) {
            this.preparedStatement = connection.prepareStatement(this.builder.toString());
            if (this.needsReplacements) {
                for (int i = 0; i < this.replacements.length; i++) {
                    this.preparedStatement.setObject(i + 1, this.replacements[i]);
                }
            }
            return this.preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Error while trying to execute a statement: ", e);
        } finally {
            this.done();
        }
    }

    public CompletableFuture<Integer> executeUpdateAsync() {
        return CompletableFuture.supplyAsync(this::executeUpdate, Database.getExecutorService()).exceptionally(e -> {
            e.printStackTrace();
            return 0;
        });
    }

    public void executeQuery(Consumer<QueryResult> consumer) {
        Preconditions.checkNonNull(consumer, "Consumer can't be null.");
        try (Connection connection = Database.getConnection()) {
            this.preparedStatement = connection.prepareStatement(this.builder.toString());
            if (this.needsReplacements) {
                for (int i = 0; i < this.replacements.length; i++) {
                    this.preparedStatement.setObject(i + 1, this.replacements[i]);
                }
            }
            try (ResultSet resultSet = this.preparedStatement.executeQuery()) {
                consumer.accept(new QueryResult(resultSet));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error while trying to execute a statement: ", e);
        }
    }

    public CompletableFuture<Void> executeQueryAsync(Consumer<QueryResult> consumer) {
        return CompletableFuture.runAsync(() -> this.executeQuery(consumer), Database.getExecutorService()).exceptionally(e -> {
            e.printStackTrace();
            return null;
        });
    }

    public void done() {
        try {
            if (!this.preparedStatement.isClosed()) {
                this.preparedStatement.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error while trying to close a statement: ", e);
        }
    }

    /**
     * Creates a new statement out of a raw statement string.
     *
     * @param of the raw statement string.
     * @return a new statement instance.
     */
    public static Statement create(String of) {
        return new Statement().of(of);
    }

    public static Statement create() {
        return new Statement();
    }
}
