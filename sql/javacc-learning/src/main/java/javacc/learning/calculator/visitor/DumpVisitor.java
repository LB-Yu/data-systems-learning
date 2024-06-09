package javacc.learning.calculator.visitor;

import javacc.learning.calculator.ast.*;

public class DumpVisitor implements ASTVisitor<Void> {

  private int level = 0;

  public void dump(Node node) {
    node.accept(this);
  }

  @Override
  public Void visit(ExprNode node) {
    printIndent(level);
    System.out.println(node.getOp());

    ++level;
    node.getLeft().accept(this);
    node.getRight().accept(this);
    --level;
    return null;
  }

  @Override
  public Void visit(TermNode node) {
    printIndent(level);
    System.out.println(node.getOp());

    ++level;
    node.getLeft().accept(this);
    node.getRight().accept(this);
    --level;
    return null;
  }

  @Override
  public Void visit(SinNode node) {
    printIndent(level);
    System.out.println("sin");

    ++level;
    node.getNode().accept(this);
    --level;
    return null;
  }

  @Override
  public Void visit(CosNode node) {
    printIndent(level);
    System.out.println("cos");

    ++level;
    node.getNode().accept(this);
    --level;
    return null;
  }

  @Override
  public Void visit(TanNode node) {
    printIndent(level);
    System.out.println("tan");

    ++level;
    node.getNode().accept(this);
    --level;
    return null;
  }

  @Override
  public Void visit(FactorialNode node) {
    printIndent(level);
    System.out.println("!");

    ++level;
    node.getNode().accept(this);
    --level;
    return null;
  }

  @Override
  public Void visit(ValueNode node) {
    printIndent(level);
    System.out.println(node);
    return null;
  }

  public void printIndent(int level) {
    for (int i = 0; i < level * 2; ++i) {
      System.out.print(" ");
    }
  }
}
