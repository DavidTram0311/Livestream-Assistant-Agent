import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import *
import logging
from .utils import is_effectively_empty

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s')

logging.info(f"Starting SentimentExtract")

def get_spark_session(gpu: bool = False, apple_silicon: bool = False) -> SparkSession:
    if apple_silicon:
        return sparknlp.start(gpu=gpu, aarch64=True)

    return sparknlp.start(gpu=gpu)

class SentimentExtract:
    def __init__(
        self, 
        input_col: str, 
        model_name: str, 
        encoder_name: str, 
        gpu: bool = False,
        apple_silicon: bool = False,
        spark: SparkSession = None
        ):

        self.input_col = input_col
        self.model_name = model_name
        self.encoder_name = encoder_name

        if spark is None:
            logging.info(f"Creating new Spark session")
            self.spark = get_spark_session(gpu=gpu, apple_silicon=apple_silicon)
        else:
            self.spark = spark

        # Load and fit the pipeline ONLY ONCE
        pipeline_model = self._load_and_fit_pipeline()
        self.light_pipeline = LightPipeline(pipeline_model)

    def _load_and_fit_pipeline(self) -> Pipeline:
        # 1. Document Assembler
        logging.info(f"Loading pipeline for model: {self.model_name} and encoder: {self.encoder_name}")
        try:
            document_assembler = DocumentAssembler() \
                .setInputCol(self.input_col) \
                .setOutputCol("document")
        except Exception as e:
            logging.error(f"Error loading document assembler: {e}")
            raise e

        # 2. Sentence Encoder
        logging.info(f"Loading sentence encoder for model: {self.encoder_name}")
        try:
            sentence_encoder = UniversalSentenceEncoder.pretrained(name=self.encoder_name, lang="en") \
                .setInputCols(["document"]) \
                .setOutputCol("sentence_embeddings")
        except Exception as e:
            logging.error(f"Error loading sentence encoder: {e}")
            raise e
        
        # 3. Sentiment DL Model
        logging.info(f"Loading sentiment DL model for model: {self.model_name}")
        try:
            sentiment_dl_model = SentimentDLModel.pretrained(name=self.model_name, lang="en") \
                .setInputCols(["sentence_embeddings"]) \
                .setOutputCol("sentiment")
        except Exception as e:
            logging.error(f"Error loading sentiment DL model: {e}")
            raise e
        
        # 4. Pipeline
        logging.info(f"Loading pipeline")
        try:
            pipeline = Pipeline(
                stages=[
                    document_assembler, 
                    sentence_encoder, 
                    sentiment_dl_model
                    ])
        except Exception as e:
            logging.error(f"Error loading pipeline: {e}")
            raise e

        logging.info(f"Pipeline loaded successfully")

        # 5. Fit the pipeline
        logging.info(f"Fitting the pipeline")
        empty_df = self.spark.createDataFrame([[""]], [self.input_col])
        try:
            pipeline_model = pipeline.fit(empty_df)
        except Exception as e:
            logging.error(f"Error fitting the pipeline: {e}")
            raise e

        logging.info(f"Pipeline fitted successfully")
        return pipeline_model

    def predict(self, text: str):
        """
        Returns the sentiment prediction for the given text.
        """
        result = self.light_pipeline.annotate(text)
        
        if is_effectively_empty(text):
            return "empty_text"

        if "sentiment" in result and len(result["sentiment"]) > 0:
            return result["sentiment"][0]

        return "unknown"