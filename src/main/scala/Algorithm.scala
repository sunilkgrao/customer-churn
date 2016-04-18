import grizzled.slf4j.Logger
import hex.deeplearning.{DeepLearning, DeepLearningModel, DeepLearningParameters}
import io.prediction.controller.{IPersistentModel, P2LAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.h2o._

case class AlgorithmParams(epochs: Int) extends Params

class Algorithm(val ap: AlgorithmParams)
  // extends PAlgorithm if Model contains RDD[]
  extends P2LAlgorithm[PreparedData, Model, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): Model = {

    val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    //
    // -- TODO: more ways to filter & combine data
    //
    val customerTable: H2OFrame = new H2OFrame(data.customers)

    //
    // -- Run DeepLearning
    //
    val dlParams = new DeepLearningParameters()
    dlParams._train = customerTable
    dlParams._response_column = 'churn
    dlParams._epochs = ap.epochs

    val dl = new DeepLearning(dlParams)
    new Model(dlModel = dl.trainModel.get, sc = sc, h2oContext = h2oContext)

  }

  def predict(model: Model, query: Query): PredictedResult = {

    //
    // -- Make prediction
    //
    import model.h2oContext._

    // Build customer from query and convert into data frame
    val customerFromQuery = Customer(
      None, Some(query.opportunityId), Some(query.accountId),
      Some(query.name),
      Some(query.description), Some(query.amount),
      Some(query.closeDate), Some(query.opportunityType),
      Some(query.nextStep), Some(query.leadSource),
      None, Some(query.campaignId),
      Some(query.ownerId), Some(query.territory),
      Some(query.createdDate), Some(query.fiscalQuarter),
      Some(query.fiscalYear),Some(query.discount),Some(query.competitor),Some(query.age))

    val customerQueryRDD = model.sc.parallelize(Array(customerFromQuery))
    val customerQueryDataFrame = new H2OFrame(customerQueryRDD)

    // Predict using the data frame made
    val predictionH2OFrame = model.dlModel.score(customerQueryDataFrame)('predict)
    val predictionFromModel = toRDD[DoubleHolder](predictionH2OFrame)
      .map( _.result.getOrElse(Double.NaN) ).collect

    PredictedResult(p = predictionFromModel(0).toString)
  }
}

class Model(val dlModel: DeepLearningModel, val sc: SparkContext,
            val h2oContext: H2OContext)
  extends IPersistentModel[AlgorithmParams] {

  // Sparkling water models are not deserialization-friendly
  def save(id: String, params: AlgorithmParams, sc: SparkContext): Boolean = {
    false
  }
}
