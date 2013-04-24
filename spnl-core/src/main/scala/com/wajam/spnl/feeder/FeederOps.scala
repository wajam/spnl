package com.wajam.spnl.feeder

/**
 * Class to add more methods on top of any existing Feeder implementation.
 */

case class FeederOps(feeder: Feeder) {

  /**
   * Add filtering capability to a feeder.
   *
   * @param predicate the predicate to use to filter data. If the predicate returns true, the data is returned.
   *                  Otherwise, the data is skipped.
   * @return a FilteredFeeder
   */
  def withFilter(predicate: Feeder.FeederPredicate) = {
    new FilteredFeeder(feeder, predicate)
  }
}

/**
 * A class to add predicate composition features
 *
 * @param predicate the initial predicate
 */
case class FeederFilter(predicate: Feeder.FeederPredicate) {
  def ||(otherPredicate: Feeder.FeederPredicate) = {
    FeederFilter(data => (this.predicate(data) || otherPredicate(data)))
  }

  def &&(otherPredicate: Feeder.FeederPredicate) = {
    FeederFilter(data => (this.predicate(data) && otherPredicate(data)))
  }
}
