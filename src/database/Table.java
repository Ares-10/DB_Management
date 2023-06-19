package database;

import java.util.function.Predicate;

public interface Table extends Joinable {
    // 테이블의 이름을 반환
    String getName();

    // 테이블 헤더 + 내용 출력
    void show();

    // 테이블 요약 정보 출력
    void describe();

    /**
     * @return 처음 (최대)5개 행으로 구성된 새로운 Table 생성 후 반환
     */
    Table head();
    /**
     * @return 처음 (최대)lineCount개 행으로 구성된 새로운 Table 생성 후 반환
     */
    Table head(int lineCount);

    /**
     * @return 마지막 (최대)5개 행으로 구성된 새로운 Table 생성 후 반환
     */
    Table tail();
    /**
     * @return 마지막 (최대)lineCount개 행으로 구성된 새로운 Table 생성 후 반환
     */
    Table tail(int lineCount);

    /**
     * @param beginIndex 포함(이상)
     * @param endIndex 미포함(미만)
     * @return 검색 범위에 해당하는 행으로 구성된 새로운 Table 생성 후 반환
     * 존재하지 않는 행 인덱스 전달시 예외 발생
     */
    Table selectRows(int beginIndex, int endIndex);

    /**
     * @return 검색 인덱스에 해당하는 행으로 구성된 새로운 Table 생성 후 반환
     * 결과 테이블의 행 출현 순서는 매개변수 인덱스 순서와 동일함
     * 존재하지 않는 행 인덱스 전달시 예외 발생
     */
    Table selectRowsAt(int ...indices);

    /**
     * @param beginIndex 포함(이상)
     * @param endIndex 미포함(미만)
     * @return 검색 범위에 해당하는 열로 구성된 새로운 Table 생성 후 반환
     * 존재하지 않는 열 인덱스 전달시 예외 발생
     */
    Table selectColumns(int beginIndex, int endIndex);

    /**
     * @return 검색 인덱스에 해당하는 열로 구성된 새로운 Table 생성 후 반환
     * 결과 테이블의 열 출현 순서는 매개변수 인덱스 순서와 동일함
     * 존재하지 않는 열 인덱스 전달시 예외 발생
     */
    Table selectColumnsAt(int ...indices);

    /**
     * @param
     * @return 검색 조건에 해당하는 행으로 구성된 새로운 Table 생성 후 반환
     */
    <T> Table selectRowsBy(String columnName, Predicate<T> predicate);

    /**
     * @return 원본 Table이 정렬되어 반환된다.
     * @param byIndexOfColumn 정렬 기준 컬럼, 존재하지 않는 컬럼 인덱스 전달시 예외 발생
     */
    Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst);

    int getRowCount();
    int getColumnCount();

    /**
     * @return 원본 Column이 반환된다. 따라서, 반환된 Column에 대한 조작은 원본 Table에 영향을 끼친다.
     */
    Column getColumn(int index);
    /**
     * @return 원본 Column이 반환된다. 따라서, 반환된 Column에 대한 조작은 원본 Table에 영향을 끼친다.
     */
    Column getColumn(String name);
}
