package database;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

class TableImpl implements Table {

    private String tableName;
    private Column[] cols;

    TableImpl(String tableName, Column[] cols) {
        this.tableName = tableName;
        this.cols = cols;
    }

    TableImpl(String tableName, Table table) {
        this.tableName = tableName;
        cols = new Column[table.getColumnCount()];
        for (int i = 0; i < table.getColumnCount(); i++) cols[i] = table.getColumn(i);
    }

    void addCol(List<Object> col, String value) {
        String data;
        if (value == null) data = new String("");
        else data = new String(value);
        try {
            col.add(Integer.parseInt(data));
        } catch (Exception e) {
            if (data.equals(""))
                col.add(null);
            else
                col.add(data);
        }
    }

    List<Integer> innerJoinRowsList(Table rightTable, List<JoinColumn> joinColumns, Table crossJoinTable) {
        List<Integer> list = new ArrayList<>();
        Column leftColumn = crossJoinTable.getColumn(tableName + "." + joinColumns.get(0).getColumnOfThisTable());
        Column rightColumn = crossJoinTable.getColumn(rightTable.getName() + "." + joinColumns.get(0).getColumnOfAnotherTable());
        if (leftColumn == null || rightColumn == null) return null;
        for (int i = 0; i < crossJoinTable.getRowCount(); i++) {
            if (leftColumn.getValue(i) == null || rightColumn.getValue(i) == null) continue;
            if (leftColumn.getValue(i).equals(rightColumn.getValue(i))) list.add(i);
        }
        return list;
    }

    @Override
    public Table crossJoin(Table rightTable) {
        String name = new String(tableName + " CrossJoin " + rightTable.getName());

        // Column 이름 설정
        String[] headers = new String[cols.length + rightTable.getColumnCount()];
        for (int i = 0; i < cols.length + rightTable.getColumnCount(); i++) {
            if (i < cols.length) headers[i] = getName() + "." + cols[i].getHeader();
            else headers[i] = rightTable.getName() + "." + rightTable.getColumn(i - cols.length).getHeader();
        }

        // 리턴할 테이블의 Column 생성
        Column[] table = new ColumnImpl[headers.length];

        // left 테이블 추가
        for (int i = 0; i < cols.length; i++) {
            List<Object> col = new ArrayList<>();
            for (int j = 0; j < getRowCount(); j++)
                for (int k = 0; k < rightTable.getRowCount(); k++)
                    addCol(col, cols[i].getValue(j));
            table[i] = new ColumnImpl(headers[i], col);
        }

        // right 테이블 추가
        for (int i = 0; i < rightTable.getColumnCount(); i++) {
            List<Object> col = new ArrayList<>();
            for (int j = 0; j < getRowCount(); j++)
                for (int k = 0; k < rightTable.getRowCount(); k++)
                    addCol(col, rightTable.getColumn(i).getValue(k));
            table[i + cols.length] = new ColumnImpl(headers[i + cols.length], col);
        }

        return new TableImpl(name, table);
    }

    @Override
    public Table innerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        String name = new String(tableName + " InnerJoin " + rightTable.getName());
        Table crossJoinTable = crossJoin(rightTable);
        List<Integer> list = innerJoinRowsList(rightTable, joinColumns, crossJoinTable);
        return new TableImpl(name, crossJoinTable.selectRowsAt(list.stream().mapToInt(Integer::intValue).toArray()));
    }

    @Override
    public Table outerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        String name = new String(tableName + " OuterJoin " + rightTable.getName());

        // innerJoin 부분 생성
        Table crossJoinTable = crossJoin(rightTable);
        List<Integer> list = innerJoinRowsList(rightTable, joinColumns, crossJoinTable);

        // outerJoin 부분 생성
        List<Integer> missingNumbers = new ArrayList<>();
        for (int i = 0; i < getRowCount(); i++) {
            boolean found = false;
            for (int j = 0; j < rightTable.getRowCount(); j++) {
                if (list.contains(i * rightTable.getRowCount() + j)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                missingNumbers.add(i * rightTable.getRowCount());
            }
        }
        list.addAll(missingNumbers);
        Table outerJoinTable = new TableImpl(name, crossJoinTable.selectRowsAt(list.stream().mapToInt(Integer::intValue).toArray()));

        // outerJoin 부분 초기화
        for (int i = (int) (list.stream().count() - missingNumbers.stream().count()); i < outerJoinTable.getRowCount(); i++) {
            for (int j = cols.length; j < outerJoinTable.getColumnCount(); j++) {
                outerJoinTable.getColumn(j).setValue(i, "");
            }
        }

        return outerJoinTable;
    }

    @Override
    public Table fullOuterJoin(Table rightTable, List<JoinColumn> joinColumns) {
        String name = new String(tableName + " FullOuterJoin " + rightTable.getName());

        // innerJoin 부분 생성
        Table crossJoinTable = crossJoin(rightTable);
        List<Integer> list = innerJoinRowsList(rightTable, joinColumns, crossJoinTable);

        // fullOuterJoin 부분 생성
        List<Integer> leftOuter = new ArrayList<>();
        for (int i = 0; i < getRowCount(); i++) {
            boolean found = false;
            for (int j = 0; j < rightTable.getRowCount(); j++) {
                if (list.contains(i * rightTable.getRowCount() + j)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                leftOuter.add(i * rightTable.getRowCount());
            }
        }
        List<Integer> rightOuter = new ArrayList<>();
        for (int j = 0; j < rightTable.getRowCount(); j++) {
            boolean found = false;
            for (int i = 0; i < getRowCount(); i++) {
                if (list.contains(i * rightTable.getRowCount() + j)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                rightOuter.add(j);
            }
        }
        list.addAll(leftOuter);
        list.addAll(rightOuter);
        Table fullOuterJoinTable = new TableImpl(name, crossJoinTable.selectRowsAt(list.stream().mapToInt(Integer::intValue).toArray()));

        // fullOuterJoin 부분 초기화
        for (int i = (int) (list.stream().count() - leftOuter.stream().count() - rightOuter.stream().count()); i < fullOuterJoinTable.getRowCount(); i++) {
            if (i < list.stream().count() - rightOuter.stream().count())
                for (int j = cols.length; j < fullOuterJoinTable.getColumnCount(); j++) {
                    fullOuterJoinTable.getColumn(j).setValue(i, "");
                }
            else for (int j = 0; j < cols.length; j++) {
                fullOuterJoinTable.getColumn(j).setValue(i, "");
            }
        }

        return fullOuterJoinTable;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void show() {
        int[] max = new int[cols.length];
        for (int i = 0; i < cols.length; i++) {
            max[i] = cols[i].getHeader().length();
            for (int j = 0; j < cols[i].count(); j++) {
                if (cols[i].getValue(j) == null) {
                    if (max[i] < 4)
                        max[i] = 4;
                } else if (max[i] < cols[i].getValue(j).length()) max[i] = cols[i].getValue(j).length();
            }
        }
        for (int j = 0; j < cols.length; j++) {
            for (int k = 0; k < max[j] - cols[j].getHeader().length(); k++) System.out.print(" ");
            System.out.print(cols[j].getHeader() + " | ");
        }
        System.out.println();
        for (int i = 0; i < cols[0].count(); i++) {
            for (int j = 0; j < cols.length; j++) {
                if (cols[j].getValue(i) == null) {
                    for (int k = 0; k < max[j] - 4; k++) System.out.print(" ");
                    System.out.print("null | ");
                } else {
                    for (int k = 0; k < max[j] - cols[j].getValue(i).length(); k++) System.out.print(" ");
                    System.out.print(cols[j].getValue(i) + " | ");
                }
            }
            System.out.println();
        }
    }

    @Override
    public void describe() {
        System.out.println("<"+ toString() + ">");
        System.out.println("RangeIndex: " + cols[0].count() + " entries, 0 to " + (cols[0].count() - 1));
        System.out.println("Data columns (total " + cols.length + " columns):");

        int max = 6, countInt = 0, countStr = 0;
        for (int i = 0; i < cols.length; i++) {
            if (max < cols[i].getHeader().length()) max = cols[i].getHeader().length();
        }
        System.out.print("# |");
        for (int i = 0; i < max - 6; i++) System.out.print(" ");
        System.out.println("Column |Non-Null Count |Dtype");

        for (int i = 0; i < cols.length; i++) {
            System.out.print(i + " |");
            for (int j = 0; j < max - cols[i].getHeader().length(); j++) System.out.print(" ");
            System.out.print(cols[i].getHeader() + " |    " + (cols[i].count() - cols[i].getNullCount()) + " non-null |");
            if (cols[i].isNumericColumn()) {
                countInt++;
                System.out.println("int");
            } else {
                countStr++;
                System.out.println("String");
            }
        }
        System.out.println("dtypes: int(" + countInt + "), String(" + countStr + ")");
    }

    @Override
    public Table head() {
        return head(5);
    }

    @Override
    public Table head(int lineCount) {
        if (lineCount > this.cols[0].count()) lineCount = this.cols[0].count();
        Column[] cols = new ColumnImpl[this.cols.length];
        for (int i = 0; i < this.cols.length; i++) {
            List<Object> col = new ArrayList<>();
            for (int j = 0; j < lineCount; j++) addCol(col, this.cols[i].getValue(j));
            cols[i] = new ColumnImpl(this.cols[i].getHeader(), col);
        }
        return new TableImpl(this.tableName, cols);
    }

    @Override
    public Table tail() {
        return tail(5);
    }

    @Override
    public Table tail(int lineCount) {
        if (lineCount > this.cols[0].count()) lineCount = this.cols[0].count();
        Column[] cols = new ColumnImpl[this.cols.length];
        for (int i = 0; i < this.cols.length; i++) {
            List<Object> col = new ArrayList<>();
            for (int j = this.cols[0].count() - lineCount; j < this.cols[0].count(); j++)
                addCol(col, this.cols[i].getValue(j));
            cols[i] = new ColumnImpl(this.cols[i].getHeader(), col);
        }
        return new TableImpl(this.tableName, cols);
    }

    @Override
    public Table selectRows(int beginIndex, int endIndex) {
        Column[] cols = new ColumnImpl[this.cols.length];
        for (int i = 0; i < this.cols.length; i++) {
            List<Object> col = new ArrayList<>();
            for (int j = beginIndex; j < endIndex; j++) addCol(col, this.cols[i].getValue(j));
            cols[i] = new ColumnImpl(this.cols[i].getHeader(), col);
        }
        return new TableImpl(this.tableName, cols);
    }

    @Override
    public Table selectRowsAt(int... indices) {
        Column[] cols = new ColumnImpl[this.cols.length];
        for (int i = 0; i < this.cols.length; i++) {
            List<Object> col = new ArrayList<>();
            for (int j : indices) addCol(col, this.cols[i].getValue(j));
            cols[i] = new ColumnImpl(this.cols[i].getHeader(), col);
        }
        return new TableImpl(this.tableName, cols);
    }

    @Override
    public Table selectColumns(int beginIndex, int endIndex) {
        Column[] cols = new ColumnImpl[endIndex - beginIndex];
        for (int i = beginIndex; i < endIndex; i++) {
            List<Object> col = new ArrayList<>();
            for (int j = 0; j < this.cols[0].count(); j++) addCol(col, this.cols[i].getValue(j));
            cols[i] = new ColumnImpl(this.cols[i].getHeader(), col);
        }
        return new TableImpl(this.tableName, cols);
    }

    @Override
    public Table selectColumnsAt(int... indices) {
        Column[] cols = new ColumnImpl[indices.length];
        for (int i = 0; i < indices.length; i++) {
            List<Object> col = new ArrayList<>();
            for (int j = 0; j < this.cols[0].count(); j++) addCol(col, this.cols[indices[i]].getValue(j));
            cols[i] = new ColumnImpl(this.cols[indices[i]].getHeader(), col);
        }
        return new TableImpl(this.tableName, cols);
    }

    @Override
    public <T> Table selectRowsBy(String columnName, Predicate<T> predicate) {
        String name = new String(tableName + " SelectRowsBy " + columnName);
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < getRowCount(); i++) {
            try {
                if (predicate.test((T) getColumn(columnName).getValue(i))) list.add(i);
            } catch (Exception e) {
                if (predicate.test((T) getColumn(columnName).getValue(i, Integer.class))) list.add(i);
            }
        }
        return new TableImpl(name, selectRowsAt(list.stream().mapToInt(Integer::intValue).toArray()));
    }

    @Override
    public Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        if (!cols[byIndexOfColumn].isNumericColumn()) return this; //
        if (!isNullFirst) {
            int rowPos = cols[0].count() - (int) cols[byIndexOfColumn].getNullCount();
            for (int i = 0; i < cols[0].count() - (int) cols[byIndexOfColumn].getNullCount(); i++) {
                if (cols[byIndexOfColumn].getValue(i) == null) {
                    swapRows(i--, rowPos++); // 바뀐 값이 null일 수도 있으니 작업 위치부터 다시 정렬
                }
            }
            sorting(byIndexOfColumn, isAscending, 0, cols[0].count() - (int) cols[byIndexOfColumn].getNullCount());
        } else {
            int rowPos = 0;
            for (int i = 0; i < cols[0].count(); i++) {
                if (cols[byIndexOfColumn].getValue(i) == null) {
                    swapRows(i, rowPos++);
                }
            }
            sorting(byIndexOfColumn, isAscending, rowPos, cols[0].count());
        }
        return this;
    }

    private void sorting(int byIndexOfColumn, boolean isAscending, int start, int end) {
        for (int i = start; i < end; i++) {
            int p = i;
            for (int j = i; j < end; j++) {
                if (isAscending) {
                    if (cols[byIndexOfColumn].getValue(p, Integer.class) > cols[byIndexOfColumn].getValue(j, Integer.class))
                        p = j;
                } else {
                    if (cols[byIndexOfColumn].getValue(p, Integer.class) < cols[byIndexOfColumn].getValue(j, Integer.class))
                        p = j;
                }
            }
            swapRows(p, i);
        }
    }

    private void swapRows(int index, int rowPos) {
        for (int j = 0; j < cols.length; j++) {
            String temp1 = cols[j].getValue(index);
            if (temp1 == null) temp1 = "";
            String temp2 = cols[j].getValue(rowPos);
            if (temp2 == null) temp2 = "";
            cols[j].setValue(index, temp2);
            cols[j].setValue(rowPos, temp1);
        }
    }

    @Override
    public int getRowCount() {
        return cols[0].count();
    }

    @Override
    public int getColumnCount() {
        return cols.length;
    }

    @Override
    public Column getColumn(int index) {
        return cols[index];
    }

    @Override
    public Column getColumn(String name) {
        for (int i = 0; i < cols.length; i++) {
            if (name.equals(cols[i].getHeader())) return cols[i];
        }
        return null;
    }
}
