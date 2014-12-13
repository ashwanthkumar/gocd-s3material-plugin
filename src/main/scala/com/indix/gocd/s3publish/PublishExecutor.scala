package com.indix.gocd.s3publish

import java.util.{Map => JMap}

import com.thoughtworks.go.plugin.api.response.execution.ExecutionResult
import com.thoughtworks.go.plugin.api.task.{TaskConfig, TaskExecutionContext, TaskExecutor}
import material.store.S3ArtifactStore
import org.apache.hadoop.conf.Configuration

class PublishExecutor extends TaskExecutor {
  override def execute(config: TaskConfig, context: TaskExecutionContext): ExecutionResult = {
    val environment = context.environment().asMap()
    if (!checkForAccessKeyId(environment)) return ExecutionResult.failure("AWS_ACCESS_KEY_ID environment variable not present")
    if (!checkForSecretKey(environment)) return ExecutionResult.failure("AWS_SECRET_ACCESS_KEY environment variable not present")

    val bucket = config.getValue(PublishTask.BUCKET_NAME)
    val source = config.getValue(PublishTask.SOURCE)
    val s3Store = S3ArtifactStore(configWithS3Keys(environment, bucket))
    s3Store.put(s"${context.workingDir()}/$source", "")
    ExecutionResult.success("Yay! We're good to go now I guess =D")
  }

  private def destinationOnS3(environment: JMap[String, String]) = {
    val pipeline = environment.get("GO_PIPELINE_NAME")
    val pipelineCounter = environment.get("GO_PIPELINE_COUNTER")
    val stageName = environment.get("GO_STAGE_NAME")
    val stageCounter = environment.get("GO_STAGE_COUNTER")

    // FIXME - May be make this configurable?
    s"${pipeline}_$stageName/${pipelineCounter}_$stageCounter"
  }

  private def configWithS3Keys(environment: JMap[String, String], bucket: String) = {
    val config = new Configuration()
    val secretKey = environment.get(PublishTask.AWS_SECRET_ACCESS_KEY)
    val accessId = environment.get(PublishTask.AWS_ACCESS_KEY_ID)

    config.set("fs.defaultFS", s"s3://$bucket")

    config.set("fs.s3.awsSecretAccessKey", secretKey)
    config.set("fs.s3.awsAccessKeyId", accessId)

    //    config.set("fs.s3n.awsAccessKeyId", accessId)
    //    config.set("fs.s3n.awsSecretAccessKey", secretKey)
    config
  }

  private def checkForAccessKeyId(environment: JMap[String, String]) = environment.containsKey(PublishTask.AWS_ACCESS_KEY_ID)

  private def checkForSecretKey(environment: JMap[String, String]) = environment.containsKey(PublishTask.AWS_SECRET_ACCESS_KEY)
}
