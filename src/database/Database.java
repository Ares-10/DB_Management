package database;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class Database {
    // 테이블명이 같으면 같은 테이블로 간주된다.
    private static final Set<Table> tables = new HashSet<>();

    // 테이블 이름 목록을 출력한다.
    public static void showTables() {
        for(Table table : tables){
            System.out.println(table.getName());
        }
    }

    /**
     * 파일로부터 테이블을 생성하고 table에 추가한다.
     *
     * @param csv 확장자는 csv로 가정한다.
     *            파일명이 테이블명이 된다.
     *            csv 파일의 1행은 컬럼명으로 사용한다.
     *            csv 파일의 컬럼명은 중복되지 않는다고 가정한다.
     *            컬럼의 데이터 타입은 int 아니면 String으로 판정한다.
     *            String 타입의 데이터는 ("), ('), (,)는 포함하지 않는 것으로 가정한다.
     */
    public static void createTable(File csv) throws FileNotFoundException {
        Scanner sc = new Scanner(csv);
        String name = csv.getName();
        name = name.substring(0, name.length()-4);
        String[] headers = sc.nextLine().split(",", -1);
        List<String[]> rows = new ArrayList<>();
        while (sc.hasNext()) {
            rows.add(sc.nextLine().split(",", -1));
        }
        Column[] table = new ColumnImpl[headers.length];
        for(int i=0;i<headers.length;i++){
            List<Object> col = new ArrayList<>();

            for(int j=0;j<rows.size();j++){
                try {
                    col.add(Integer.parseInt(rows.get(j)[i]));
                } catch (NumberFormatException e) {
                    if (rows.get(j)[i].equals(""))
                        col.add(null);
                    else
                        col.add(rows.get(j)[i]);
                }
            }
            table[i] = new ColumnImpl(headers[i], col);
        }
        Table newTable = new TableImpl(name, table);
        tables.add(newTable);
    }

    // tableName과 테이블명이 같은 테이블을 리턴한다. 없으면 null 리턴.
    public static Table getTable(String tableName) {
        for(Table table : tables){
            if(tableName.equals(table.getName()))
                return table;
        }
        return null;
    }

    /**
     * @return 정렬된 새로운 Table 객체를 반환한다. 즉, 첫 번째 매개변수 Table은 변경되지 않는다.
     * @param byIndexOfColumn 정렬 기준 컬럼, 존재하지 않는 컬럼 인덱스 전달시 예외 발생
     */
    public static Table sort(Table table, int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        return table.head(table.getRowCount()).sort(byIndexOfColumn, isAscending, isNullFirst);
    }
}