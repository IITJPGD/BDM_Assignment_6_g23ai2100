
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import io.github.cdimascio.dotenv.Dotenv;

public class Main {

    private Connection con;

    //ans1
    public Connection connect() throws SQLException {
        Dotenv dotenv = Dotenv.load();
        // AWS Redshift connection details
        String url = dotenv.get("DB_URL");
        String uid = dotenv.get("DB_USERNAME");
        String pw = dotenv.get("DB_PASSWORD");

        try {
            System.out.println("Connecting to database...");
            con = DriverManager.getConnection(url, uid, pw);
            System.out.println("Connected successfully.");
            return con;
        } catch (SQLException e) {
            System.err.println("Connection failed: " + e.getMessage());
            throw e;
        }
    }

    //ans2
    public void drop() throws SQLException {
        String[] dropStatements = {
            "DROP TABLE IF EXISTS stockprice",
            "DROP TABLE IF EXISTS company"
        };

        try (Statement stmt = con.createStatement()) {
            for (String dropStatement : dropStatements) {
                stmt.executeUpdate(dropStatement);
            }
            System.out.println("Tables dropped successfully");
        }
    }

    //ans3
    public void create() throws SQLException {
        String[] createTableStatements = {
            """
            CREATE TABLE IF NOT EXISTS company (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                ticker CHAR(10),
                annualRevenue DECIMAL(15,2),
                numEmployees INT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS stockprice (
                companyId INT,
                priceDate DATE,
                openPrice DECIMAL(10,2),
                highPrice DECIMAL(10,2),
                lowPrice DECIMAL(10,2),
                closePrice DECIMAL(10,2),
                volume INT,
                PRIMARY KEY (companyId, priceDate),
                FOREIGN KEY (companyId) REFERENCES company(id)
            )
            """
        };

        try (Statement stmt = con.createStatement()) {
            for (String createStatement : createTableStatements) {
                stmt.executeUpdate(createStatement);
            }
            System.out.println("Tables created successfully");
        } catch (SQLException e) {
            // Handle SQL exceptions
            System.err.println("SQL Error occurred while creating tables: "
                    + e.getMessage());
            e.printStackTrace(); // Log the stack trace for debugging purposes
        } catch (Exception e) {
            // Handle any other unexpected exceptions
            System.err.println("Unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    //ans4
    public void insert() throws SQLException {
        String[] insertStatements = {
            """
            INSERT INTO company (id, name, ticker, annualRevenue, numEmployees)
            VALUES
            (1, 'Apple', 'AAPL', 387540000000.00, 154000),
            (2, 'GameStop', 'GME', 611000000.00, 12000),
            (3, 'Handy Repair', NULL, 2000000, 50),
            (4, 'Microsoft', 'MSFT', 198270000000.00, 221000),
            (5, 'StartUp', NULL, 50000, 3)
            """,
            """
            INSERT INTO stockprice (companyId, priceDate, openPrice, highPrice, lowPrice, closePrice, volume)
            VALUES
            (1, '2022-08-15', 171.52, 173.39, 171.35, 173.19, 54091700),
            (1, '2022-08-16', 172.78, 173.71, 171.66, 173.03, 56377100),
            (1, '2022-08-17', 172.77, 176.15, 172.57, 174.55, 79542000),
            (1, '2022-08-18', 173.75, 174.90, 173.12, 174.15, 62290100),
            (1, '2022-08-19', 173.03, 173.74, 171.31, 171.52, 70211500),
            (1, '2022-08-22', 169.69, 169.86, 167.14, 167.57, 69026800),
            (1, '2022-08-23', 167.08, 168.71, 166.65, 167.23, 54147100),
            (1, '2022-08-24', 167.32, 168.11, 166.25, 167.53, 53841500),
            (1, '2022-08-25', 168.78, 170.14, 168.35, 170.03, 51218200),
            (1, '2022-08-26', 170.57, 171.05, 163.56, 163.62, 78823500),
            (1, '2022-08-29', 161.15, 162.90, 159.82, 161.38, 73314000),
            (1, '2022-08-30', 162.13, 162.56, 157.72, 158.91, 77906200),
            (2, '2022-08-15', 39.75, 40.39, 38.81, 39.68, 5243100),
            (2, '2022-08-16', 39.17, 45.53, 38.60, 42.19, 23602800),
            (2, '2022-08-17', 42.18, 44.36, 40.41, 40.52, 9766400),
            (2, '2022-08-18', 39.27, 40.07, 37.34, 37.93, 8145400),
            (2, '2022-08-19', 35.18, 37.19, 34.67, 36.49, 9525600),
            (2, '2022-08-22', 34.31, 36.20, 34.20, 34.50, 5798600),
            (2, '2022-08-23', 34.70, 34.99, 33.45, 33.53, 4836300),
            (2, '2022-08-24', 34.00, 34.94, 32.44, 32.50, 5620300),
            (2, '2022-08-25', 32.84, 32.89, 31.50, 31.96, 4726300),
            (2, '2022-08-26', 31.50, 32.38, 30.63, 30.94, 4289500),
            (2, '2022-08-29', 30.48, 32.75, 30.38, 31.55, 4292700),
            (2, '2022-08-30', 31.62, 31.87, 29.42, 29.84, 5060200),
            (4, '2022-08-15', 291.00, 294.18, 290.11, 293.47, 18085700),
            (4, '2022-08-16', 291.99, 294.04, 290.42, 292.71, 18102900),
            (4, '2022-08-17', 289.74, 293.35, 289.47, 291.32, 18253400),
            (4, '2022-08-18', 290.19, 291.91, 289.08, 290.17, 17186200),
            (4, '2022-08-19', 288.90, 289.25, 285.56, 286.15, 20557200),
            (4, '2022-08-22', 282.08, 282.46, 277.22, 277.75, 25061100),
            (4, '2022-08-23', 276.44, 278.86, 275.40, 276.44, 17527400),
            (4, '2022-08-24', 275.41, 277.23, 275.11, 275.79, 18137000),
            (4, '2022-08-25', 277.33, 279.02, 274.52, 278.85, 16583400),
            (4, '2022-08-26', 279.08, 280.34, 267.98, 268.09, 27532500),
            (4, '2022-08-29', 265.85, 267.40, 263.85, 265.23, 20338500),
            (4, '2022-08-30', 266.67, 267.05, 260.66, 262.97, 22767100)
            """
        };

        try (Statement stmt = con.createStatement()) {
            for (String insertStatement : insertStatements) {
                stmt.executeUpdate(insertStatement);
            }
            System.out.println("Data inserted successfully");
        } catch (SQLException e) {
            // Handle SQL exceptions
            System.err.println("SQL Error occurred while creating tables: "
                    + e.getMessage());
            e.printStackTrace(); // Log the stack trace for debugging purposes
        } catch (Exception e) {
            // Handle any other unexpected exceptions
            System.err.println("Unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    //ans5
    public void delete() throws SQLException {
        String deleteStockPrices = """
        DELETE FROM stockprice
        WHERE priceDate < '2022-08-20' OR companyId = 2
        """;
        try (Statement stmt = con.createStatement()) {
            int rowsDeleted = stmt.executeUpdate(deleteStockPrices);
            System.out.println(rowsDeleted + " records deleted");
        } catch (SQLException e) {
            // Handle SQL exceptions
            System.err.println("SQL Error occurred while creating tables: "
                    + e.getMessage());
            e.printStackTrace(); // Log the stack trace for debugging purposes
        } catch (Exception e) {
            // Handle any other unexpected exceptions
            System.err.println("Unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    //ans6
    public ResultSet query1() throws SQLException {
        System.out.println("Executing query #1.");

        // SQL Query to get the most recent top 10 orders with total sale and order date for customers in America
        String query1SQL = """
        SELECT O.O_ORDERKEY, O.O_TOTALPRICE, O.O_ORDERDATE
        FROM dev.orders O
        JOIN dev.customer C ON O.O_CUSTKEY = C.C_CUSTKEY
        JOIN dev.nation N ON C.C_NATIONKEY = N.N_NATIONKEY
        WHERE N.N_NAME = 'UNITED STATES'
        ORDER BY O.O_ORDERDATE DESC
        LIMIT 10
        """;

        // Execute query
        Statement stmt = con.createStatement();
        return stmt.executeQuery(query1SQL);
    }

    //ans7
    public ResultSet queryTwo() throws SQLException {
        String query = """
        SELECT c.name, c.ticker,
               MIN(sp.lowPrice) AS lowestPrice,
               MAX(sp.highPrice) AS highestPrice,
               AVG(sp.closePrice) AS avgClosingPrice,
               AVG(sp.volume) AS avgVolume
        FROM company c
        JOIN stockprice sp ON c.id = sp.companyId
        WHERE sp.priceDate BETWEEN '2022-08-22' AND '2022-08-26'
        GROUP BY c.name, c.ticker
        ORDER BY avgVolume DESC
        """;
        Statement stmt = con.createStatement();
        return stmt.executeQuery(query);
    }

    //ans 8
    public ResultSet queryThree() throws SQLException {
        String query = """
        SELECT c.name, c.ticker, sp.closePrice
        FROM company c
        LEFT JOIN stockprice sp ON c.id = sp.companyId
        WHERE sp.priceDate = '2022-08-30'
        AND (sp.closePrice <= 1.10 * (
            SELECT AVG(sp1.closePrice)
            FROM stockprice sp1
            WHERE sp1.companyId = sp.companyId
            AND sp1.priceDate BETWEEN '2022-08-15' AND '2022-08-19'
        ) OR c.ticker IS NULL)
        ORDER BY c.name ASC
        """;
        Statement stmt = con.createStatement();
        return stmt.executeQuery(query);
    }
}
