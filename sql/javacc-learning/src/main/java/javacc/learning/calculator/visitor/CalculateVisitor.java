package javacc.learning.calculator.visitor;

import javacc.learning.calculator.ast.*;

public class CalculateVisitor implements ASTVisitor<Double> {

  public double calculate(Node node) {
    return node.accept(this);
  }

  @Override
  public Double visit(ExprNode node) {
    double leftValue = node.getLeft().accept(this);
    double rightValue = node.getRight().accept(this);
    switch (node.getOp()) {
      case PLUS:
        return (leftValue + rightValue) * node.getSign();
      case MINUS:
        return (leftValue - rightValue) * node.getSign();
      default:
        throw new IllegalArgumentException("Illegal operator for expr node");
    }
  }

  @Override
  public Double visit(TermNode node) {
    double leftValue = node.getLeft().accept(this);
    double rightValue = node.getRight().accept(this);
    switch (node.getOp()) {
      case MUL:
        return leftValue * rightValue * node.getSign();
      case DIV:
        return leftValue / rightValue * node.getSign();
      default:
        throw new IllegalArgumentException("Illegal operator for term node");
    }
  }

  @Override
  public Double visit(SinNode node) {
    return Math.sin(node.getNode().accept(this)) * node.getSign();
  }

  @Override
  public Double visit(CosNode node) {
    return Math.cos(node.getNode().accept(this)) * node.getSign();
  }

  @Override
  public Double visit(TanNode node) {
    return Math.tan(node.getNode().accept(this)) * node.getSign();
  }

  @Override
  public Double visit(FactorialNode node) {
    double value = node.getNode().accept(this);
    double result = 1;
    for (int i = 1; i <= value; ++i) {
      result *= i;
    }
    return result * node.getSign();
  }

  @Override
  public Double visit(ValueNode node) {
    return node.getValue() * node.getSign();
  }
}
