package javacc.learning.calculator.visitor;

import javacc.learning.calculator.ast.*;

public interface ASTVisitor<T> {

  T visit(ExprNode node);

  T visit(TermNode node);

  T visit(SinNode node);

  T visit(CosNode node);

  T visit(TanNode node);

  T visit(FactorialNode node);

  T visit(ValueNode node);

}
