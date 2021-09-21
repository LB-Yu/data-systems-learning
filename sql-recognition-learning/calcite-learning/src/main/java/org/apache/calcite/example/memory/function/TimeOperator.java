package org.apache.calcite.example.memory.function;

import java.sql.Date;
import java.util.Calendar;

public class TimeOperator {
    public int THE_YEAR(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(Calendar.YEAR);
    }

    public Integer THE_MONTH(Date date) {
        return 6;
    }

    public Integer THE_DAY(Date date) {
        return 16;
    }

    public Integer THE_SYEAR(Date date, String type) {
        return 18;
    }
}
