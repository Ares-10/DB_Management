package database;

import java.util.List;

public interface Joinable {
    Table crossJoin(Table rightTable);
    Table innerJoin(Table rightTable, List<JoinColumn> joinColumns);

    Table outerJoin(Table rightTable, List<JoinColumn> joinColumns);

    Table fullOuterJoin(Table rightTable, List<JoinColumn> joinColumns);
}
