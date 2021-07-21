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

import java.io.File
import java.util.Optional

import org.json4s.JValue

import org.apache.spark.SparkException

private[spark] object ExecutionErrors {
  def acquireAnAddressNotExistError(resourceName: String, address: String): Throwable = {
    new SparkException(s"Try to acquire an address that doesn't exist. $resourceName " +
      s"address $address doesn't exist.")
  }

  def acquireAnAddressNotAvailableError(resourceName: String, address: String): Throwable = {
    new SparkException("Try to acquire an address that is not available. " +
      s"$resourceName address $address is not available.")
  }

  def releaseAnAddressNotExistError(resourceName: String, address: String): Throwable = {
    new SparkException(s"Try to release an address that doesn't exist. $resourceName " +
      s"address $address doesn't exist.")
  }

  def releaseAnAddressNotAssignedError(resourceName: String, address: String): Throwable = {
    new SparkException(s"Try to release an address that is not assigned. $resourceName " +
      s"address $address is not assigned.")
  }

  def resourceScriptNotExistError(scriptFile: File, resourceName: String): Throwable = {
    new SparkException(s"Resource script: $scriptFile to discover $resourceName " +
      "doesn't exist!")
  }

  def expectUseResourceButNotSpecifyADiscoveryScriptError(resourceName: String): Throwable = {
    new SparkException(s"User is expecting to use resource: $resourceName, but " +
      "didn't specify a discovery script!")
  }

  def runningOtherResourceError(
                                 script: Optional[String],
                                 name: String,
                                 resourceName: String): Throwable = {
    new SparkException(s"Error running the resource discovery script ${script.get}: " +
      s"script returned resource name ${name} and we were expecting $resourceName.")
  }

  def ParsingJsonToResourceInformationError(
                                                  json: String,
                                                  exampleJson: String,
                                                  e: Throwable): Throwable = {
    new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n" +
      s"Here is a correct example: $exampleJson.", e)
  }

  def ParsingJsonToResourceInformationError(json: JValue, e: Throwable): Throwable = {
    new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n", e)
  }

  def resourceNotExistInProfileIdError(resource: String, id: Int): Throwable = {
    new SparkException(s"Resource $resource doesn't exist in profile id: $id")
  }

  def conditionOfExecutorResourceError(rName: String, num: Long, taskReq: Double): Throwable = {
    new SparkException(s"The executor resource: $rName, amount: ${num} " +
      s"needs to be >= the task resource request amount of $taskReq")
  }

  def noExecutorResourceConfigError(str: String): Throwable = {
    new SparkException("No executor resource configs were not specified for the " +
      s"following task configs: ${str}")
  }

  def resourceProfileSupportedError(): Throwable = {
    new SparkException("ResourceProfiles are only supported on YARN and Kubernetes " +
      "with dynamic allocation enabled.")
  }

  def resourceProfileIdNotFoundError(rpId: Int): Throwable = {
    new SparkException(s"ResourceProfileId $rpId not found!")
  }

  def specifyConfigOfResourceError(str: String): Throwable = {
    new SparkException(s"You must specify an amount for ${str}")
  }


  def specifyAnAmountConfigForResourceError(
                                             key: String,
                                             componentName: String,
                                             RESOURCE_PREFIX: String): Throwable = {
    new SparkException(s"You must specify an amount config for resource: $key " +
      s"config: $componentName.$RESOURCE_PREFIX.$key")
  }

  def conditionOfResourceAmountError(doubleAmount: Double): Throwable = {
    new SparkException(
      s"The resource amount ${doubleAmount} must be either <= 0.5, or a whole number.")
  }

  def specifyAmountForResourceError(str: String): Throwable = {
    new SparkException(s"You must specify an amount for ${str}")
  }

  def tasksSupportFractionalResourceError(componentName: String): Throwable = {
    new SparkException(
      s"Only tasks support fractional resources, please check your $componentName settings")
  }

  def ParsingResourceFileError(resourcesFile: String, e: Throwable): Throwable = {
    new SparkException(s"Error parsing resources file $resourcesFile", e)
  }

  def conditionOfTheNumberOfCoresPerExecutorError(execCores: Int, taskCpus: Int): Throwable = {
    new SparkException(s"The number of cores per executor (=$execCores) has to be >= " +
      s"the number of cpus per task = $taskCpus.")
  }

  def nonOfTheDiscoveryPluginsReturnedResourceInfoError(str: String): Throwable = {
    new SparkException(s"None of the discovery plugins returned ResourceInformation for " +
      s"${str}")
  }

  def adjustConfigurationError(
                                cores: Int,
                                taskCpus: Int,
                                resourceNumSlots: Int,
                                limitingResource: String,
                                maxTaskPerExec: Int): Throwable = {
    new SparkException(s"The configuration of cores (exec = ${cores} " +
      s"task = ${taskCpus}, runnable tasks = ${resourceNumSlots}) will " +
      s"result in wasted resources due to resource ${limitingResource} limiting the " +
      s"number of runnable tasks per executor to: ${maxTaskPerExec}. Please adjust " +
      "your configuration.")
  }

  def adjustConfigurationError(
                                uri: String,
                                execAmount: Long,
                                taskReqStr: String,
                                resourceNumSlots: Int,
                                limitingResource: String,
                                maxTaskPerExec: Int): Throwable = {
    new SparkException(s"The configuration of resource: $uri " +
      s"(exec = ${execAmount}, task = ${taskReqStr}, " +
      s"runnable tasks = ${resourceNumSlots}) will " +
      s"result in wasted resources due to resource ${limitingResource} limiting the " +
      s"number of runnable tasks per executor to: ${maxTaskPerExec}. Please adjust " +
      "your configuration.")
  }
}
