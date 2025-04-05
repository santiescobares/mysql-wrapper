package net.escosoft.mysqlwrapper.table;

import java.util.List;

public interface Table {

    /**
     * Gets the name of the table.
     *
     * @return the table's name.
     */
    String getName();

    /**
     * Gets all table's columns.
     *
     * @return a list with all table column objects.
     */
    List<TableColumn> getColumns();

    /**
     * Gets all table's options.
     *
     * @return a list with all table option strings.
     */
    List<String> getOptions();
}
