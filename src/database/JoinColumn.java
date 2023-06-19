package database;

public class JoinColumn {
    private final String columnOfThisTable;
    private final String columnOfAnotherTable;

    public JoinColumn(String columnOfThisTable, String columnOfAnotherTable) {
        this.columnOfThisTable = columnOfThisTable;
        this.columnOfAnotherTable = columnOfAnotherTable;
    }

    public String getColumnOfThisTable() {
        return columnOfThisTable;
    }

    public String getColumnOfAnotherTable() {
        return columnOfAnotherTable;
    }

    public JoinColumn getSwoppedJoinColumn() {
        return new JoinColumn(columnOfAnotherTable, columnOfThisTable);
    }
}
