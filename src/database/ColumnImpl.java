package database;

import java.util.List;

class ColumnImpl implements Column {

    private String header;
    List<Object> col;

    ColumnImpl(String header, List<Object> col) {
        this.header = header;
        this.col = col;
    }

    @Override
    public String getHeader() {
        return header;
    }

    @Override
    public String getValue(int index) {
        if (col.get(index) == null) return null;
        return col.get(index).toString();
    }

    @Override
    public <T extends Number> T getValue(int index, Class<T> t) {
        Object value = col.get(index);
        if (value == null) {
            return null;
        }
        if (t == Double.class) {
            return (T) Double.valueOf(value.toString());
        } else if (t == Long.class) {
            return (T) Long.valueOf(value.toString());
        } else if (t == Integer.class) {
            return (T) Integer.valueOf(value.toString());
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void setValue(int index, String value) {
        if (value.equals("")) col.set(index, null);
        else {
            try {
                setValue(index, Integer.parseInt(value));
            } catch (Exception e) {
                col.set(index, value);
            }
        }
    }

    @Override
    public void setValue(int index, int value) {
        col.set(index, value);
    }

    @Override
    public int count() {
        return col.size();
    }

    @Override
    public void show() {
        for (int i = 0; i < col.size(); i++)
            System.out.println(col.get(i));
    }

    @Override
    public boolean isNumericColumn() {
        for (int i = 0; i < col.size(); i++) {
            if (col.get(i) != null && (col.get(i) instanceof Integer) || (col.get(i) instanceof Double)) continue;
            if (col.get(i) == null) continue;
            return false;
        }
        return true;
    }

    @Override
    public long getNullCount() {
        int count = 0;
        for (int i = 0; i < col.size(); i++) {
            if (col.get(i) == null) count++;
        }
        return count;
    }
}
