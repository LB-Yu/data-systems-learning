package javacc.learning.calculator.ast;

import javacc.learning.calculator.visitor.ASTVisitor;

public class ValueNode extends Node {

  protected double value;

  public ValueNode(double value) {
    this.value = value;
  }

  public double getValue() {
    return value;
  }

  @Override
  public <T> T accept(ASTVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return (sign == 1 ? "" : "-") + value;
  }
}
