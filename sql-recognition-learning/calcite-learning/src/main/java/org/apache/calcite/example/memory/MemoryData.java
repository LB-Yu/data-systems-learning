package org.apache.calcite.example.memory;

import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;

public class MemoryData {
    public static final Map<String, Database> MAP = new HashMap<>();
    public static Map<String, SqlTypeName> SQLTYPE_MAPPING = new HashMap<>();
    public static Map<String, Class> JAVATYPE_MAPPING = new HashMap<>();

    static {
        initRowType();
        Database school = new Database();
        Table student = new Table();
        initStudentTable(student);

        Table classs = new Table();
        initClassTable(classs);

        school.tables.add(student);
        school.tables.add(classs);
        MAP.put("school", school);
    }

    public static void initRowType() {
        SQLTYPE_MAPPING.put("char", SqlTypeName.CHAR);
        JAVATYPE_MAPPING.put("char", Character.class);
        SQLTYPE_MAPPING.put("varchar", SqlTypeName.VARCHAR);
        JAVATYPE_MAPPING.put("varchar", String.class);
        SQLTYPE_MAPPING.put("boolean", SqlTypeName.BOOLEAN);
        SQLTYPE_MAPPING.put("integer", SqlTypeName.INTEGER);
        JAVATYPE_MAPPING.put("integer", Integer.class);
        SQLTYPE_MAPPING.put("tinyint", SqlTypeName.TINYINT);
        SQLTYPE_MAPPING.put("smallint", SqlTypeName.SMALLINT);
        SQLTYPE_MAPPING.put("bigint", SqlTypeName.BIGINT);
        SQLTYPE_MAPPING.put("decimal", SqlTypeName.DECIMAL);
        SQLTYPE_MAPPING.put("numeric", SqlTypeName.DECIMAL);
        SQLTYPE_MAPPING.put("float", SqlTypeName.FLOAT);
        SQLTYPE_MAPPING.put("real", SqlTypeName.REAL);
        SQLTYPE_MAPPING.put("double", SqlTypeName.DOUBLE);
        SQLTYPE_MAPPING.put("date", SqlTypeName.DATE);
        JAVATYPE_MAPPING.put("date", Date.class);
        SQLTYPE_MAPPING.put("time", SqlTypeName.TIME);
        SQLTYPE_MAPPING.put("timestamp", SqlTypeName.TIMESTAMP);
        SQLTYPE_MAPPING.put("any", SqlTypeName.ANY);
    }

    public static void initClassTable(Table cl) {
        cl.tableName = "Class";
        Column name = new Column();
        name.name = "name";
        name.type = "varchar";
        cl.columns.add(name);

        Column id = new Column();
        id.name = "id";
        id.type = "integer";
        cl.columns.add(id);

        Column teacher = new Column();
        teacher.name = "teacher";
        teacher.type = "varchar";
        cl.columns.add(teacher);

        cl.data.add(Arrays.asList("3-1", "1", "fengsu"));
        cl.data.add(Arrays.asList("3-2", "2", "sunshue"));
        cl.data.add(Arrays.asList("3-3", "3", "sunshdh"));
        cl.data.add(Arrays.asList("3-4", "4", "shwud"));
    }

    public static void initStudentTable(Table student) {
        student.tableName = "Student";
        Column name = new Column();
        name.name = "name";
        name.type = "varchar";
        student.columns.add(name);

        Column id = new Column();
        id.name = "id";
        id.type = "varchar";
        student.columns.add(id);

        Column classId = new Column();
        classId.name = "classId";
        classId.type = "integer";
        student.columns.add(classId);

        Column birth = new Column();
        birth.name = "birthday";
        birth.type = "date";
        student.columns.add(birth);

        Column home = new Column();
        home.name = "home";
        home.type = "varchar";
        student.columns.add(home);

        student.data.add(Arrays.asList("fengysh","A000001", "1", "1989-06-10", "anhui"));
        student.data.add(Arrays.asList("wyshz","A000002", "1", "1989-03-04", "henan"));
        student.data.add(Arrays.asList("hesk","A000003", "1", "1992-02-10", "anhui"));
        student.data.add(Arrays.asList("whst","A000004", "2", "1993-04-08", "hebei"));
        student.data.add(Arrays.asList("wush","B000005", "2", "1998-02-26", "beijing"));
        student.data.add(Arrays.asList("ehsn","C000006", "3", "1990-06-18", "sichuan"));
        student.data.add(Arrays.asList("wisyh","D000007", "3", "1991-03-06", "zhejiang"));
        student.data.add(Arrays.asList("helsj","D000008", "4", "1993-09-10", "jiangsu"));
    }

    public static class Database {
        public List<Table> tables = new LinkedList<>();
    }


    public static class Table {
        public String tableName;
        public List<Column> columns = new LinkedList<>();
        public List<List<String>> data = new LinkedList<>();
    }

    public static class Column {
        public String name;
        public String type;
    }
}
