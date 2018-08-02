package com.github.vangj.dp.model

/**
  * Conditional probabilty filters. If we have 2 events, A and B, then the conditional
  * probability of A given B is P(A|B) = P(A, B) / P (B) where
  * <ul>
  *   <li>P(A, B) = N_AB / N</li>
  *   <li>P(B) = N_B / N</li>
  *   <li>N_AB is the number of times A and B occur together</li>
  *   <li>N_B is the number of times B occurs</li>
  *   <li>N is the total number of events</li>
  * </ul>
  * The numerator filter only gets N_AB and the denominator filter only gets N_B. You
  * will need a separate query to get N and separate operations to compute the actual
  * conditional probability.
  * @param numerator Numerator filter.
  * @param denominator Denominator filter.
  */
case class CondProbFilter(numerator: String, denominator: String)
