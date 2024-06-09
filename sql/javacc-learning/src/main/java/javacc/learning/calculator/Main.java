package javacc.learning.calculator;

import javacc.learning.calculator.ast.Node;
import javacc.learning.calculator.parser.Calculator;
import javacc.learning.calculator.parser.ParseException;
import javacc.learning.calculator.visitor.CalculateVisitor;
import javacc.learning.calculator.visitor.DumpVisitor;

public class Main {

  public static void main(String[] args) throws ParseException {
    Calculator calculator = new Calculator(System.in);
    Node node = calculator.parse();

    CalculateVisitor calculateVisitor = new CalculateVisitor();
    System.out.println(calculateVisitor.calculate(node));

    System.out.println("\nAST dump:");
    DumpVisitor dumpVisitor = new DumpVisitor();
    dumpVisitor.dump(node);
  }
}
