package com.github.vangj.dp.model.filter

class CpFilter(numerator: UdfFilter, denominator: UdfFilter) {
  def getNumUdfFilter(): UdfFilter = numerator
  def getDenUdfFilter(): UdfFilter = denominator
  def getCondProb(n: Double): Double = {
    if (getDenUdfFilter().numOfVariables() > 0) {
      numerator.count / denominator.count
    } else {
      numerator.count / n
    }
  }
}
