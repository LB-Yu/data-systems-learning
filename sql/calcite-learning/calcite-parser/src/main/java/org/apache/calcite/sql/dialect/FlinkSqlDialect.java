package org.apache.calcite.sql.dialect;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

public class FlinkSqlDialect extends SqlDialect {

    public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProductName("Flink")
            .withConformance(FlinkSqlConformance.DEFAULT)
            .withIdentifierQuoteString("`")
            .withNullCollation(NullCollation.LOW);

    public static final SqlDialect DEFAULT = new SparkSqlDialect(DEFAULT_CONTEXT);

    public FlinkSqlDialect(Context context) {
        super(context);
    }

}
