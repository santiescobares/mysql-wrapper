package net.escosoft.mysqlwrapper.table;

import lombok.*;
import net.escosoft.mysqlwrapper.util.Preconditions;

import java.util.StringJoiner;

@Getter
public final class TableColumn {

    private final String name;
    private final TableType type;

    private String[] typeData;
    private boolean notNull;
    private String defaultValue;

    private TableColumn(String name, TableType type) {
        this.name = Preconditions.checkNonNull(name, "Name can't be null.");
        this.type = Preconditions.checkNonNull(type, "Type can't be null.");
    }

    /**
     * Builds the required statement syntax for creating this column on a table.
     *
     * @return the statement syntax of this column.
     */
    public String create() {
        StringBuilder builder = new StringBuilder(this.name)
                .append(" ")
                .append(this.type);
        if (this.typeData != null) {
            StringJoiner joiner = new StringJoiner(", ");
            for (String value : this.typeData) {
                joiner.add(value);
            }
            builder.append("(")
                    .append(joiner)
                    .append(")");
        }
        if (this.notNull) {
            builder.append(" NOT NULL");
        }
        if (this.defaultValue != null) {
            builder.append(" DEFAULT ")
                    .append(this.defaultValue);
        }
        return builder.toString();
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Builder implements net.escosoft.mysqlwrapper.util.Builder<TableColumn> {

        private final TableColumn column;

        /**
         * Adds additional (optional or required sometimes) data for column types such as CHAR, VARCHAR.
         *
         * @param typeData raw additional data without parenthesis.
         */
        public Builder typeData(String... typeData) {
            Preconditions.checkNonNull(typeData, "Type data can't be null.");
            Preconditions.checkLength(typeData, 1, "Type data can't be empty.");
            this.column.typeData = typeData;
            return this;
        }

        /**
         * Adds NOT NULL string to current column building-up.
         */
        public Builder notNull() {
            this.column.notNull = true;
            return this;
        }

        /**
         * Adds a DEFAULT value to current column building-up.
         *
         * @param defaultValue the default value string to add.
         */
        public Builder defaultValue(String defaultValue) {
            this.column.defaultValue = Preconditions.checkNonNull(defaultValue, "Default value can't be null.");
            return this;
        }

        @Override
        public TableColumn build() {
            return this.column;
        }

        /**
         * Creates a new table's column out of two always-required parameters.
         *
         * @param name the column's name.
         * @param type the column's type.
         * @return a new TableColumn instance.
         */
        public static Builder of(String name, TableType type) {
            return new Builder(new TableColumn(name, type));
        }
    }
}
