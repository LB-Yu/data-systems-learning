package javacc.learning.calculator.ast;

import javacc.learning.calculator.visitor.ASTVisitor;

public class FactorialNode extends UnaryNode {

  public FactorialNode(Node node) {
    this.node = node;
  }

  @Override
  public String toString() {
    if (node instanceof ValueNode) {
      return (sign == 1 ? "" : "-") + node + "!";
    } else {
      return (sign == 1 ? "" : "-") + "(" + node + ")" + "!";
    }
  }

  @Override
  public <T> T accept(ASTVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
