# app.py
from aws_cdk import (App, Stack, Duration, RemovalPolicy, aws_s3 as s3,
                     aws_iam as iam, aws_events as events,
                     aws_events_targets as targets, aws_lambda_event_sources as lambda_events,
                     aws_sqs as sqs, aws_stepfunctions as sfn,
                     aws_stepfunctions_tasks as tasks)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from aws_cdk.aws_lambda import Runtime
from constructs import Construct
import aws_cdk.aws_s3_notifications as s3n
import os

class BlsDatausaPipelineStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        account_id = Stack.of(self).account
        region = Stack.of(self).region

        bucket_name = f"rearc-quest-{account_id}-{region}"
        bucket = s3.Bucket(self, "DataSyncBucket", 
                           bucket_name=bucket_name,
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True,
                           public_read_access=True,
                           block_public_access=s3.BlockPublicAccess(block_public_policy=False,
                                                                     block_public_acls=False,
                                                                     ignore_public_acls=False,
                                                                     restrict_public_buckets=False))

        # Public read and list permissions
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    f"{bucket.bucket_arn}",
                    f"{bucket.bucket_arn}/*"
                ],
                principals=[iam.AnyPrincipal()]
            )
        )

        # Common Lambda role
        lambda_role = iam.Role(self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )

        env_vars = {
            "BUCKET_NAME": bucket.bucket_name,
            "S3_PREFIX": "bls/"
        }

        bls_lambda = PythonFunction(self, "BlsFileSyncFunction",
            entry="lambda/bls_file_sync",
            runtime=Runtime.PYTHON_3_11,
            index="bls_file_sync.py",
            handler="lambda_handler",
            environment=env_vars,
            role=lambda_role,
            timeout=Duration.seconds(60)
        )

        datausa_lambda = PythonFunction(self, "FetchDataUSAFn",
            entry="lambda/fetch_datausa_population",
            runtime=Runtime.PYTHON_3_11,
            index="fetch_datausa_population.py",
            handler="lambda_handler",
            environment=env_vars,
            role=lambda_role,
            timeout=Duration.seconds(30)
        )

        # Step Function for sequential execution
        step1 = tasks.LambdaInvoke(self, "Sync BLS Files",
            lambda_function=bls_lambda,
            output_path="$.Payload"
        )

        step2 = tasks.LambdaInvoke(self, "Fetch DataUSA Population",
            lambda_function=datausa_lambda,
            output_path="$.Payload"
        )

        definition = step1.next(step2)

        state_machine = sfn.StateMachine(self, "BlsDatausaStateMachine",
            definition=definition,
            timeout=Duration.minutes(5)
        )

        # Daily trigger for Step Function at 6 AM EST (11:00 UTC)
        rule = events.Rule(self, "DailyTriggerStepFunction",
            schedule=events.Schedule.cron(minute="0", hour="11")
        )
        rule.add_target(targets.SfnStateMachine(state_machine))

        # SQS Queue for S3 notifications
        notification_queue = sqs.Queue(self, "PopulationJsonQueue",
            visibility_timeout=Duration.seconds(60),
            retention_period=Duration.days(1)
        )

        # Allow S3 to send messages to SQS
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(notification_queue),
            s3.NotificationKeyFilter(prefix="datausa/population.json")
        )

        # Lambda to process SQS messages
        reporting_lambda = PythonFunction(self, "ReportGenerationFunction",
            entry="lambda/report_generator",
            runtime=Runtime.PYTHON_3_11,
            index="report_generator.py",
            handler="lambda_handler",
            environment=env_vars,
            role=lambda_role,
            timeout=Duration.seconds(60),
            bundling={
                "asset_excludes": ["pandas/tests", "pandas/io/tests", "__pycache__"]
            }
        )

        reporting_lambda.add_event_source(lambda_events.SqsEventSource(notification_queue))

app = App()
BlsDatausaPipelineStack(app, "BlsDatausaPipelineStack")
app.synth()