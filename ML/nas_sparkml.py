from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline

# Spark 세션 생성
spark = SparkSession.builder.appName("StockPricePrediction").getOrCreate()

# 데이터 불러오기
data = spark.read.csv("path_to_your_data.csv", header=True, inferSchema=True)

# 데이터 전처리: 특성 열 생성
feature_cols = ["Open", "High", "Low", "Volume"]  # 사용할 특성 열
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
data = vector_assembler.transform(data)

# 데이터 분할: 학습 데이터와 테스트 데이터로 분할
train_data, test_data = data.randomSplit([0.8, 0.2], seed=123)

# 모델 생성 및 학습
regressor = RandomForestRegressor(featuresCol="features", labelCol="Close", numTrees=10)
pipeline = Pipeline(stages=[regressor])
model = pipeline.fit(train_data)

# 테스트 데이터로 예측 수행
predictions = model.transform(test_data)

# 예측 결과 확인
predictions.select("Close", "prediction").show()

# Spark 세션 종료
spark.stop()
