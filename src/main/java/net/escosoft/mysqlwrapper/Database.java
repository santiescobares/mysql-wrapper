package net.escosoft.mysqlwrapper;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import net.escosoft.mysqlwrapper.table.Table;
import net.escosoft.mysqlwrapper.table.TableColumn;
import net.escosoft.mysqlwrapper.util.Builder;
import net.escosoft.mysqlwrapper.util.Preconditions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Database {

    private String host;
    private int port = -1;
    private String name, username, password;
    private Options options;

    private boolean credentialsSet;

    private final HikariConfig config = new HikariConfig();
    private static HikariDataSource dataSource;
    @Getter(AccessLevel.PROTECTED)
    private static ExecutorService executorService;

    /**
     * Sets the host of the database connection.
     *
     * @param host the database host as string.
     */
    public Database host(String host) {
        Preconditions.checkNonNull(host, "Host can't be null.");
        this.host = host;
        this.checkCredentials();
        return this;
    }

    /**
     * Sets the port of the database connection.
     *
     * @param port the database port.
     */
    public Database port(int port) {
        Preconditions.checkRange(port, 1, 65535, "Port must be between 1 and 65535.");
        this.port = port;
        this.checkCredentials();
        return this;
    }

    public Database port(String port) {
        Preconditions.checkNonNull(port, "Port can't be null.");
        return this.port(Integer.parseInt(port));
    }

    /**
     * Sets the name of the database to connect to.
     *
     * @param name the database name.
     */
    public Database name(String name) {
        Preconditions.checkNonNull(name, "Name can't be null.");
        this.name = name;
        this.checkCredentials();
        return this;
    }

    /**
     * Sets the username to authenticate into this database with.
     *
     * @param username the database username.
     */
    public Database username(String username) {
        Preconditions.checkNonNull(username, "Username can't be null.");
        this.username = username;
        this.checkCredentials();
        return this;
    }

    /**
     * Sets the password to authenticate into this database with.
     *
     * @param password the database password.
     */
    public Database password(String password) {
        Preconditions.checkNonNull(password, "Password can't be null.");
        this.password = password;
        this.checkCredentials();
        return this;
    }

    /**
     * Adds an optional set of options that will be put into the database creation URI.
     *
     * @param options an options object.
     */
    public Database withOptions(Options options) {
        Preconditions.checkNonNull(options, "Options can't be null.");
        this.options = options;
        return this;
    }

    /**
     * Sets the data source class name provided by JDBC driver.
     *
     * @param dataSourceName the data source class name.
     */
    public Database dataSourceName(String dataSourceName) {
        Preconditions.checkNonNull(dataSourceName, "Data source name can't be null.");
        this.config.setDataSourceClassName(dataSourceName);
        return this;
    }

    /**
     * Sets a name to the database pool.
     *
     * @param poolName the database pool name.
     */
    public Database poolName(String poolName) {
        Preconditions.checkNonNull(poolName, "Pool name can't be null.");
        this.config.setPoolName(poolName);
        return this;
    }

    /**
     * Sets the maximum amount of connections that the database pool may open.
     *
     * @param maxPoolSize the maximum amount of connections.
     */
    public Database maxPoolSize(int maxPoolSize) {
        this.config.setMaximumPoolSize(maxPoolSize);
        return this;
    }

    /**
     * Sets a timeout for the client to await for a database connection.
     *
     * @param millis the time in milliseconds.
     */
    public Database connectionTimeout(long millis) {
        this.config.setConnectionTimeout(millis);
        return this;
    }

    /**
     * Sets a timeout for a connection to sit idle in the pool.
     *
     * @param millis the time in milliseconds.
     */
    public Database idleTimeout(long millis) {
        this.config.setIdleTimeout(millis);
        return this;
    }

    /**
     * Sets a time for the database to keep alive connections from pool.
     *
     * @param millis the time in milliseconds.
     */
    public Database keepAlive(long millis) {
        this.config.setKeepaliveTime(millis);
        return this;
    }

    /**
     * Sets the maximum lifetime of a connection in the pool.
     *
     * @param millis the time in milliseconds.
     */
    public Database maxLifetime(long millis) {
        this.config.setMaxLifetime(millis);
        return this;
    }

    /**
     * Controls the minimum amount of idle connections to maintain in the pool.
     *
     * @param minIdle the minimum amount of idle connections.
     */
    public Database minIdle(int minIdle) {
        this.config.setMinimumIdle(minIdle);
        return this;
    }

    /**
     * Sets a particular {@link ExecutorService} implementation that will be used for running
     * asynchronous operations on {@link Statement}.
     * Default implementation is a {@link java.util.concurrent.ThreadPoolExecutor}.
     *
     * @param service the executor service implementation.
     */
    public Database executorService(ExecutorService service) {
        executorService = service;
        return this;
    }

    /**
     * Creates a new database connection using credentials set.
     */
    public void connect() {
        if (dataSource != null) {
            throw new IllegalStateException("Database is already connected.");
        }
        if (!this.credentialsSet) {
            throw new IllegalArgumentException("You must set database credentials in order to connect to it.");
        }

        String url = "jdbc:mysql://" + this.host + ":" + this.port + "/" + this.name;
        if (this.options != null) {
            url += this.options.build();
        }

        this.config.setJdbcUrl(url);
        this.config.setUsername(this.username);
        this.config.setPassword(this.password);
        dataSource = new HikariDataSource(this.config);

        if (executorService == null) {
            executorService = Executors.newCachedThreadPool();
        }
    }

    /**
     * Shutdowns current database connection.
     */
    public static void shutdown() {
        check();
        dataSource.close();
        dataSource = null;
    }

    /**
     * Creates a table implementation in the database.
     *
     * @param table the table to create.
     */
    public static void createTable(Table table) {
        check();

        StringBuilder builder = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(table.getName())
                .append("(");
        StringJoiner joiner = new StringJoiner(", ");
        for (TableColumn column : table.getColumns()) {
            joiner.add(column.create());
        }
        for (String option : table.getOptions()) {
            joiner.add(option);
        }
        builder.append(joiner)
                .append(");");

        Statement.create(builder.toString()).executeUpdate();
    }

    /**
     * Creates multiple table implementations in the database.
     *
     * @param tables the tables to create.
     */
    public static void createTables(Table... tables) {
        for (Table table : tables) {
            createTable(table);
        }
    }

    private void checkCredentials() {
        this.credentialsSet = this.host != null && this.port != -1 && this.name != null && this.username != null && this.password != null;
    }

    /**
     * Gets a new connection from the database once it's open.
     *
     * @return a new database connection handled by HikariCP.
     * @throws SQLException that will be held in {@link Statement} instances.
     */
    public static Connection getConnection() throws SQLException {
        check();
        return dataSource.getConnection();
    }

    /**
     * Creates a new database builder.
     *
     * @return a new database builder instance.
     */
    public static Database builder() {
        return new Database();
    }

    private static void check() {
        if (dataSource == null) {
            throw new IllegalStateException("Database is not connected.");
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Options implements Builder<String> {

        private final StringBuilder trail = new StringBuilder();
        private boolean oneAdded;

        /**
         * Adds a pair of options to the database URI.
         *
         * @param name  the option name.
         * @param value the option value.
         */
        public Options append(String name, String value) {
            Preconditions.checkNonNull(name, "Name can't be null.");
            Preconditions.checkNonNull(value, "Value can't be null.");
            this.trail.append(oneAdded ? "&" : "?")
                    .append(name)
                    .append("=")
                    .append(value);
            this.oneAdded = true;
            return this;
        }

        @Override
        public String build() {
            return this.trail.toString();
        }

        /**
         * Creates a new builder for database options.
         *
         * @return a new database options builder instance.
         */
        public static Options builder() {
            return new Options();
        }
    }
}
