package javacc.learning.calculator.ast;

import javacc.learning.calculator.visitor.ASTVisitor;

public class TanNode extends UnaryNode {

  public TanNode(Node node) {
    this.node = node;
  }

  @Override
  public <T> T accept(ASTVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
