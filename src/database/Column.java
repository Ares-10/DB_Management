package database;

public interface Column {
    String getHeader();

    /* cell 값을 String으로 반환 */
    String getValue(int index);

    /**
     * @param index
     * @param t Double.class, Long.class, Integer.class
     * @return cell 값을 타입 t로 반환, cell 값이 null이면 null 반환, 타입 t로 변환 불가능한 존재하는 값에 대해서는 예외 발생
     */
    <T extends Number> T getValue(int index, Class<T> t);

    void setValue(int index, String value);

    /**
     * @param value int 리터럴을 index의 cell 값으로 건네고 싶을 때 사용
     */
    void setValue(int index, int value);

    /**
     * @return null 포함 모든 cell 개수 반환
     */
    int count();

    void show();

    /**
     * @return (int or null)로 구성된 컬럼 or (double or null)로 구성된 컬럼이면 true 반환
     */
    boolean isNumericColumn();

    long getNullCount();
}
