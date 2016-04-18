import io.prediction.controller.{Engine, IEngineFactory}
import org.joda.time.format.DateTimeFormat

case class Query(
opportunityId: String,
accountId: String,
name: String,
description: String,
amount: Double,
closeDate: DateTimeFormat,
opportunityType: String,
nextStep: String,
leadSource: String,
isWon: Boolean,
campaignId: String,
ownerId: String,
territory: String,
createdDate: DateTimeFormat,
fiscalQuarter: String,
fiscalYear: String,
discount: Double,
competitor: String,
age: Double) extends Serializable

case class PredictedResult(p: String) extends Serializable

object VanillaEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("algo" -> classOf[Algorithm]),
      classOf[Serving])
  }
}