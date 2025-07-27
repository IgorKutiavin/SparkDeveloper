package ru.kiv.spark

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{col, when}

object IrisModeler {

  def Modeller (df:DataFrame,species:String):Array[PipelineStage] = {

    val numCol = df.dtypes
      .filter(p => p._2.equals("DoubleType") || p._2.equals("IntegerType"))
      .map(_._1)

    // целевая колонка
    val dft = df.withColumn("target", when(col("species") === species, 1)
      .otherwise(0))

    dft.select("species", "target").show(5, truncate = false)

    // проверяем сбалансированность данных
    //dft.groupBy("target").count().show()

    // Работа с признаками
    // проверим корреляцию числовых данныч
    // подбираем пары колонок
    val numColsPairs = numCol.flatMap(f1 => numCol.map(f2 => (f1, f2)))
    val pairs = numColsPairs
      .filter(p => !p._1.equals(p._2))
      .map(p => if (p._1 < p._2) (p._1, p._2) else (p._2, p._1))
      .distinct
    // ищем колонки с корреляцией больше 0.7
    val corr = pairs
      .map { p => (p._1, p._2, df.stat.corr(p._1, p._2)) }
      .filter(p => scala.math.abs(p._3) > 0.7)

    corr.sortBy(_._3).reverse.foreach(c => println(f"${c._1}%25s${c._2}%25s\t${c._3}"))

    // Оставляем числовые колонки с низкой корреляцией
    val numColFinal = numCol.diff(corr.map(_._2))

    // Категориальные признаки

    // индексируем строковые колонки

    import org.apache.spark.ml.feature.StringIndexer

    val strCol = dft
      .dtypes
      .filter(_._2.equals("StringType"))
      .map(_._1)

    val strColIndexed = strCol.map(_ + "_Indexed")

    val indexer = new StringIndexer()
      .setInputCols(strCol)
      .setOutputCols(strColIndexed)

    val indexed = indexer.fit(dft).transform(dft)
    //indexed.show(5)

    // кодируем категориальные признаки
    import org.apache.spark.ml.feature.OneHotEncoder

    val catColumns = strColIndexed.map(_ + "_Coded")

    val encoder = new OneHotEncoder()
      .setInputCols(strColIndexed)
      .setOutputCols(catColumns)

    val encoderModel = encoder.fit(indexed)

    val catSizes = encoderModel.categorySizes

    val encoded = encoderModel.transform(indexed)
    //encoded.show(5)

    val codedColumns = strColIndexed.zip(catSizes.map(_ - 1)).flatMap(i => (1 to i._2).toList.map(a => i._1))

    // Собираем признаки в вектор
    val featureColumns = numColFinal ++ catColumns

    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("features")

    val assembled = assembler.transform(encoded)
    //assembled.show(5, truncate = false)

    val featureColumnsMap = (numColFinal ++ codedColumns).zipWithIndex.map(i => (i._2, i._1)).toMap
    assembled.select("features").show(5, truncate = false)

    // Нормализация
    import org.apache.spark.ml.feature.MinMaxScaler

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val scaled = scaler.fit(assembled).transform(assembled)

    scaled.select("features", "scaledFeatures").show(5, truncate = false)
//
    // Отбор признаков
    import org.apache.spark.ml.feature.UnivariateFeatureSelector

    val selector = new UnivariateFeatureSelector()
      .setFeatureType("continuous")
      .setLabelType("categorical")
      .setSelectionMode("percentile")
      .setSelectionThreshold(0.75)
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("target")
      .setOutputCol("selectedFeatures")

    val selectModel = selector.fit(scaled)
    //println(selectModel.selectedFeatures.length)

    val selectedFeaturesMap = selectModel.selectedFeatures.map(featureColumnsMap.getOrElse(_, "")).zipWithIndex.map(i => (i._2, i._1)).toMap

    val dataF = selectModel.transform(scaled)

    //dataF.select("scaledFeatures", "selectedFeatures").show(5, truncate = false)

    // Моделирование
    // Обучаюшие и тестовые выборки

    val tt: Array[Dataset[Row]] = dataF.randomSplit(Array(0.7, 0.3))
    val training = tt(0)
    val test = tt(1)

    println(s"training: $training \n test: $test")

    // логическая регрессия
    import org.apache.spark.ml.classification.LogisticRegression

    val lr = new LogisticRegression()
      .setMaxIter(1000)
      .setRegParam(0.2)
      .setElasticNetParam(0.8)
      .setFamily("auto")
      .setFeaturesCol("selectedFeatures")
      .setLabelCol("target")

    val lrModel = lr.fit(training)
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val coefficientsIndices = lrModel.coefficients.toSparse.indices.toList
    coefficientsIndices.map(i => selectedFeaturesMap.getOrElse(i, ""))

    // Завершение обучения
    val trainingSummary = lrModel.binarySummary

    println(s"accuracy: ${trainingSummary.accuracy}")
    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

    //  проверяем модель на тестовой выборке
    val predicted = lrModel.transform(test)

    predicted.select("target", "rawPrediction", "probability", "prediction").show(10, truncate = false)

    import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("target")

    println(s"areaUnderROC: ${evaluator.evaluate(predicted)}\n")

    // матрица ошибок
    val tp = predicted.filter((col("target") === 1) and (col("prediction") === 1)).count()
    val tn = predicted.filter((col("target") === 0) and (col("prediction") === 0)).count()
    val fp = predicted.filter((col("target") === 0) and (col("prediction") === 1)).count()
    val fn = predicted.filter((col("target") === 1) and (col("prediction") === 0)).count()

    println(s"Confusion Matrix:\n$tp\t$fp\n$fn\t$tn\n")

    val accuracy = (tp + tn) / (tp + tn + fp + fn).toDouble
    val precision = tp / (tp + fp).toDouble
    val recall = tp / (tp + fn).toDouble

    println(s"Accuracy = $accuracy")
    println(s"Precision = $precision")
    println(s"Recall = $recall\n")

    // Настриваем модель
    import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.01, 0.1, 0.5))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.7)
      .setParallelism(2)

    val model = trainValidationSplit.fit(dataF)
    //model.bestModel.extractParamMap()

    Array(indexer, encoder, assembler, scaler, selector, model.bestModel)
  }
}