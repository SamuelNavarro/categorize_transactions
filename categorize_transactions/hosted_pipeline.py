"""Main module."""

import sagemaker
from sagemaker.inputs import TrainingInput
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.sklearn.estimator import SKLearn
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep, TrainingStep

# Define the SageMaker execution role and session
sagemaker_session = sagemaker.Session()
role = "arn:of:your:role"

# Define pipeline parameters as needed
bucket_param = ParameterString(name="Bucket", default_value="brazilian-credit")
prefix_param = ParameterString(name="Prefix", default_value="sagemaker-demo")

# Define processors and estimator
sklearn_processor = SKLearnProcessor(
    framework_version='0.23-1',
    instance_type='ml.t3.medium',
    instance_count=1,
    base_job_name='data-pulling',
    role=role,
    sagemaker_session=sagemaker_session,
)

sklearn_estimator = SKLearn(
    entry_point='train.py',
    framework_version='0.23-1',
    instance_type='ml.m4.xlarge',
    role=role,
    sagemaker_session=sagemaker_session,
)

# Step 1: Data pulling
data_pulling_step = ProcessingStep(
    name="DataPulling",
    processor=sklearn_processor,
    outputs=[
        ProcessingOutput(
            output_name="processed_data",
            source="/opt/ml/processing/output",
            destination=f"s3://{bucket_param}/{prefix_param}/input_model",
        )
    ],
    code="data_pulling.py",
)

# Step 2: Model training
model_training_step = TrainingStep(
    name="ModelTraining",
    estimator=sklearn_estimator,
    inputs={
        "train": TrainingInput(
            s3_data=data_pulling_step.properties.ProcessingOutputConfig.Outputs["processed_data"].S3Output.S3Uri,
            content_type="application/x-parquet",
        )
    },
)

# Step 3: Prediction
predict_processor = SKLearnProcessor(
    framework_version='0.23-1',
    instance_type='ml.t3.medium',
    instance_count=1,
    base_job_name='predict',
    role=role,
    sagemaker_session=sagemaker_session,
)

predict_step = ProcessingStep(
    name="Predict",
    processor=predict_processor,
    inputs=[
        ProcessingInput(
            source=model_training_step.properties.ModelArtifacts.S3ModelArtifacts, destination="/opt/ml/model"
        ),
        ProcessingInput(
            source=data_pulling_step.properties.ProcessingOutputConfig.Outputs["processed_data"].S3Output.S3Uri,
            destination="/opt/ml/processing/input",
        ),
    ],
    outputs=[
        ProcessingOutput(
            output_name="predictions",
            source="/opt/ml/processing/output",
            destination=f"s3://{bucket_param}/{prefix_param}/output",
        )
    ],
    code="predict.py",
)

# Define and create the pipeline
pipeline = Pipeline(
    name='DemoSagemakerBelvo',
    parameters=[bucket_param, prefix_param],
    steps=[data_pulling_step, model_training_step, predict_step],
    sagemaker_session=sagemaker_session,
)

# (Optional) Update or create the pipeline in SageMaker
pipeline.upsert(role_arn=role)
