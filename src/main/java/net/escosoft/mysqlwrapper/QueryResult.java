package net.escosoft.mysqlwrapper;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import net.escosoft.mysqlwrapper.table.TableColumn;

import java.math.BigDecimal;
import java.sql.*;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public final class QueryResult {

    private final ResultSet resultSet;

    /**
     * Performs next() function from current {@link ResultSet}.
     *
     * @return the given function result.
     */
    public boolean next() {
        try {
            return this.resultSet.next();
        } catch (SQLException e) {
            throw new RuntimeException("Error while trying to get next from ResultSet: ", e);
        }
    }

    /**
     * Gets an object from a specific column of the result set.
     *
     * @param column the column instance.
     * @param type   the object type to return.
     * @return the object instance.
     */
    public <T> T get(TableColumn column, Class<T> type) {
        try {
            return this.resultSet.getObject(column.getName(), type);
        } catch (SQLException e) {
            throw new RuntimeException("Error while trying to handle a result set for column '" + column.getName() + "': ", e);
        }
    }

    public String getString(TableColumn column) {
        return this.get(column, String.class);
    }

    public int getInt(TableColumn column) {
        return this.get(column, int.class);
    }

    public double getDouble(TableColumn column) {
        return this.get(column, double.class);
    }

    public float getFloat(TableColumn column) {
        return this.get(column, float.class);
    }

    public long getLong(TableColumn column) {
        return this.get(column, long.class);
    }

    public short getShort(TableColumn column) {
        return this.get(column, short.class);
    }

    public byte getByte(TableColumn column) {
        return this.get(column, byte.class);
    }

    public Timestamp getTimestamp(TableColumn column) {
        return this.get(column, Timestamp.class);
    }

    public Time getTime(TableColumn column) {
        return this.get(column, Time.class);
    }

    public Date getDate(TableColumn column) {
        return this.get(column, Date.class);
    }

    public Blob getBlob(TableColumn column) {
        return this.get(column, Blob.class);
    }

    public Clob getClob(TableColumn column) {
        return this.get(column, Clob.class);
    }

    public BigDecimal getBigDecimal(TableColumn column) {
        return this.get(column, BigDecimal.class);
    }
}
