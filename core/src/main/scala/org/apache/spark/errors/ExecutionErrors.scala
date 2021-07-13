/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.errors

import org.apache.spark.SparkException
import org.apache.spark.shuffle.MetadataFetchFailedException

import java.io.IOException
import java.util.concurrent.{CancellationException, ExecutionException}

private[spark] object ExecutionErrors {
  def cannotFindFileSparkVersionInfoPropertiesError(): Throwable = {
    new SparkException("Could not find spark-version-info.properties")
  }

  def loadPropertiesSparkVersionInfoFileError(e: Exception): Throwable = {
    new SparkException("Error loading properties from spark-version-info.properties", e)
  }

  def closingSparkBuildInfoResourceStreamError(e: Exception): Throwable = {
    new SparkException("Error closing spark build info resource stream", e)
  }

  def dynAllocationExecutorsKeyMustBePositiveError(
      dynAllocationMinExecutorsKey: String,
      dynAllocationMaxExecutorsKey: String): Throwable = {
    new SparkException(s"$dynAllocationMinExecutorsKey and $dynAllocationMaxExecutorsKey " +
      s"must be positive!")
  }

  def dynAllocationMaxExecutorsKeyCannotBeZeroError(
      dynAllocationMaxExecutorsKey: String): Throwable = {
    new SparkException(s"$dynAllocationMaxExecutorsKey cannot be 0!")
  }

  def minNumExecutorsGreaterThanMaxNumExecutorsError(
      dynAllocationMinExecutorsKey: String,
      dynAllocationMaxExecutorsKey: String,
      minNumExecutors: Int,
      maxNumExecutors: Int): Throwable = {
    new SparkException(s"${dynAllocationMinExecutorsKey} ($minNumExecutors) must " +
      s"be less than or equal to $dynAllocationMaxExecutorsKey ($maxNumExecutors)!")
  }

  def dynAllocationSchedulerBacklogTimeoutKeyMustBeGreaterThanZeroError(
      dynAllocationSchedulerBacklogTimeoutKey: String): Throwable = {
    new SparkException(s"$dynAllocationSchedulerBacklogTimeoutKey must be > 0!")
  }

  def dynAllocationSustainedSchedulerBacklogTimeoutKeyMustBeGreaterThanZeroError(
      dynAllocationSustainedSchedulerBacklogTimeoutKey: String): Throwable = {
    new SparkException(
      s"s${dynAllocationSustainedSchedulerBacklogTimeoutKey} must be > 0!")
  }

  def externalShuffleServiceRequiredForDynamicAllocationError(): Throwable = {
    new SparkException("Dynamic allocation of executors requires the external " +
      "shuffle service. You may enable this through spark.shuffle.service.enabled.")
  }

  def dynAllocationExecutorAllocationRatioKeyOutOfRangeError(
      dynAllocationExecutorAllocationRatioKey: String): Throwable = {
    new SparkException(
      s"${dynAllocationExecutorAllocationRatioKey} must be > 0 and <= 1.0")
  }

  def unknownResourceProfileIdError(): Throwable = {
    new SparkException("ResourceProfile Id was UNKNOWN, this is not expected")
  }

  def actionHasBeenCancelledError(): Throwable = {
    new SparkException("Action has been cancelled")
  }

  def jobCancelledError(exception: Throwable): Throwable = {
    new CancellationException("Job cancelled").initCause(exception)
  }

  def jobThrowExceptionError(exception: Throwable): Throwable = {
    new ExecutionException("Exception thrown by job", exception)
  }

  def unknownSchedulerBackEndError(clazz: Object): Throwable = {
    new UnsupportedOperationException(s"Unknown scheduler backend: $clazz")
  }

  def communicateWithMapOutputTrackerError(e: Exception): Throwable = {
    new SparkException("Error communicating with MapOutputTracker", e)
  }

  def replyReceivedFromMapOutputTrackerError(response: String): Throwable = {
    new SparkException(
      "Error reply received from MapOutputTracker. Expecting true, got " + response)
  }

  def unregisterMapOutputCallForNonexistentShuffleIDError(): Throwable = {
    new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
  }

  def unregisterAllMapAndMergeOutputCallForNonexistentShuffleIDError(shuffleId: Int): Throwable = {
    new SparkException(
      s"unregisterAllMapAndMergeOutput called for nonexistent shuffle ID $shuffleId.")
  }

  def unregisterMergeResultCallForNonexistentShuffleIDError(): Throwable = {
    new SparkException("unregisterMergeResult called for nonexistent shuffle ID")
  }

  def unregisterAllMergeResultCallForNonexistentShuffleIDError(shuffleId: Int): Throwable = {
    new SparkException(
      s"unregisterAllMergeResult called for nonexistent shuffle ID $shuffleId.")
  }

  def unableToDeserializeBroadcastedMapOrMergeStatusForShuffleError(
      shuffleId: Int,
      e: SparkException): Throwable = {
    new MetadataFetchFailedException(shuffleId, -1,
      s"Unable to deserialize broadcasted map/merge statuses for shuffle $shuffleId: " + e.getCause)
  }

  def unableToDeserializeBroadcastedMapStatusForShuffleError(
      shuffleId: Int,
      e: SparkException): Throwable = {
    new MetadataFetchFailedException(shuffleId, -1,
      s"Unable to deserialize broadcasted map statuses for shuffle $shuffleId: " + e.getCause)
  }

  def unableToDeserializeBroadcastedOutputStatusError(e: IOException): Throwable = {
    new SparkException("Unable to deserialize broadcasted output statuses", e)
  }

  def missingOutputLocationForShufflePartitionError(
      shuffleId: Int,
      partition: Int,
      errorMessage: String): Throwable = {
    new MetadataFetchFailedException(shuffleId, partition, errorMessage)
  }

  def nullKeySparkConfError(): Throwable = {
    new NullPointerException("null key")
  }

  def nullValueByKeySparkConfError(key: String): Throwable = {
    new NullPointerException("null value for " + key)
  }

  def notSetParameterError(key: String): Throwable = {
    new NoSuchElementException(key)
  }

  def illegalValueAsNumberFormatExceptionError(key: String, e: NumberFormatException): Throwable = {
    new NumberFormatException(s"Illegal value for config key $key: ${e.getMessage}")
      .initCause(e)
  }

  def throwExceptionMessageError(msg: String): Throwable = {
    new Exception(msg)
  }

  def invalidSubmitDeployModeError(submitDeployModeKey: String): Throwable = {
    new SparkException(s"$submitDeployModeKey can only be \"cluster\" or \"client\".")
  }

  def missingMasterUrlConfigError(): Throwable = {
    new SparkException("A master URL must be set in your configuration")
  }

  def missingApplicationNameConfigError(): Throwable = {
    new SparkException("An application name must be set in your configuration")
  }

  def missingApplicationNameConfigError(): Throwable = {
    new SparkException("Detected yarn cluster mode, but isn't running on a cluster. " +
      "Deployment to YARN is not supported directly by SparkContext. Please use spark-submit.")
  }
}
