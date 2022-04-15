package javacc.learning.calculator.ast;

import javacc.learning.calculator.visitor.ASTVisitor;

public class TermNode extends Node {

  private final Node left;
  private final Node right;
  private final Operator op;

  public TermNode(Node left, Node right, Operator op) {
    this.left = left;
    this.right = right;
    this.op = op;
  }

  public Node getLeft() {
    return left;
  }

  public Node getRight() {
    return right;
  }

  public Operator getOp() {
    return op;
  }

  @Override
  public <T> T accept(ASTVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return (sign == 1 ? "" : "-") + String.format("%s %s %s", left.toString(), op, right.toString());
  }
}
