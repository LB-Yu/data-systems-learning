package javacc.learning.calculator.ast;

import javacc.learning.calculator.visitor.ASTVisitor;

public abstract class Node {

  protected int sign = 1;

  public int getSign() {
    return sign;
  }

  public void setSign(int sign) {
    this.sign = sign;
  }

  public abstract <T> T accept(ASTVisitor<T> visitor);
}
