package javacc.learning.calculator.ast;

public abstract class UnaryNode extends Node {

  protected Node node;

  public Node getNode() {
    return node;
  }
}
