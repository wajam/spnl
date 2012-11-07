package com.wajam.spnl

/**
 * Class used to handle different SPNL exceptions
 */
sealed abstract class SpnlResult

object SpnlOkResult extends SpnlResult

/**
 * Indicates that the action should be retried
 * @param inner Cause of exception
 */
case class SpnlRetryResult(inner: Exception) extends SpnlResult

/**
 * Indicates that the task should be killed
 * @param inner Cause of exception
 */
case class SpnlFailResult(inner: Exception) extends SpnlResult

/**
 * Indicates that the task should ignore that failure
 * @param inner Cause of exception
 */
case class SpnlIgnoreResult(inner: Exception) extends SpnlResult
