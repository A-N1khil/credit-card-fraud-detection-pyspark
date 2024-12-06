from pyspark.ml.classification import RandomForestClassificationModel, RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline, PipelineModel

class CustomPipeline:
    def __init__(self, model_path=None, pipeline_path=None):
        self.model_path = model_path
        self.pipeline_path = pipeline_path
        self.model: RandomForestClassificationModel = RandomForestClassificationModel()
        self.pipeline_model: PipelineModel = PipelineModel(stages=[])

    def __load_model(self):
        if self.model_path is None:
            self.model = RandomForestClassifier(featuresCol="scaled_features", labelCol="Class")
            print("Model not found. Training a new model.")
        else:
            print(f"Loading model from {self.model_path}")
            self.model = RandomForestClassificationModel.load(self.model_path)
            print("Model loaded.")

    def create_pipeline(self, cols, use_existing=True, use_scaler=False):
        # Create a pipeline
        if use_existing:
            print(f"Loading pipeline model from {self.pipeline_path}")
            self.pipeline_model = PipelineModel.load(self.pipeline_path)
        else:
            self.__load_model()
            assembler = VectorAssembler(inputCols=cols, outputCol="features")
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            if use_scaler:
                pipeline = Pipeline(stages=[assembler, scaler, self.model])
            else:
                pipeline = Pipeline(stages=[assembler, self.model])
            self.pipeline_model = PipelineModel(stages=pipeline.getStages())
        print("Pipeline created.")

    def transform(self, data):
        return self.pipeline_model.transform(data)

    def save_pipeline(self, path, overwrite=True):
        if overwrite:
            self.pipeline_model.write().overwrite().save(path)
        else:
            self.pipeline_model.save(path)
        print(f"Pipeline saved at {path}")
