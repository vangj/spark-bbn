package com.github.vangj.bbn.graph

import com.github.vangj.dp.model.Variable

/**
  * Conditional probability table.
  * @param node Node id.
  * @param variable Variable associated with the node.
  * @param parents List of parents.
  * @param probs List of conditional probabilities.
  */
case class Cpt(node: Int, variable: Variable, parents: List[Variable], probs: List[Double])
