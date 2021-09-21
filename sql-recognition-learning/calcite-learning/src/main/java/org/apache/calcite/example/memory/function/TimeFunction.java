package org.apache.calcite.example.memory.function;

import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;

import java.util.LinkedList;
import java.util.List;

public class TimeFunction implements Function {

    public List<FunctionParameter> getParameters() {
        List<FunctionParameter> params = new LinkedList<FunctionParameter>();
        params.add(new TimeParameter("year", 1));
        return params;
    }

}
