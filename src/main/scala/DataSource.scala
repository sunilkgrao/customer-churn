import grizzled.slf4j.Logger
import io.prediction.controller.{EmptyActualResult, EmptyEvaluationInfo, PDataSource, Params}
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // read all events of EVENT involving ENTITY_TYPE and TARGET_ENTITY_TYPE
    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("customer"),
      eventNames = Some(List("customer")))(sc)

    val customersRDD: RDD[Customer] = eventsRDD.map { event =>
      val customer = try {
        event.event match {
          case "customer" =>
            Customer(Some(event.entityId),
              Some(event.properties.get[String]("AccountId")),
              Some(event.properties.get[String]("Name")),
              Some(event.properties.get[String]("Description")),
              Some(event.properties.get[Double]("Amount")),
              Some(event.properties.get[DateTimeFormat]("CloseDate")),
              Some(event.properties.get[String]("Type")),
              Some(event.properties.get[String]("NextStep")),
              Some(event.properties.get[String]("LeadSource")),
              Some(event.properties.get[Boolean]("IsWon")),
              Some(event.properties.get[String]("CampaignId")),
              Some(event.properties.get[String]("OwnerId")),
              Some(event.properties.get[String]("Territory2Id")),
              Some(event.properties.get[DateTimeFormat]("CreatedDate")),
              Some(event.properties.get[String]("FiscalQuarter")),
              Some(event.properties.get[String]("FiscalYear")),
              Some(event.properties.get[Double]("Discount__c")),
              Some(event.properties.get[String]("Competitor__c")),
              Some(event.properties.get[Double]("Age__c")))

/*
            sb.append("Id=" + Id);
            sb.append(",AccountId=" + AccountId);
            sb.append(",Name=" + Name);
            sb.append(",Description=" + Description);
            sb.append(",Amount=" + String.valueOf(Amount));
            sb.append(",CloseDate=" + String.valueOf(CloseDate));
            sb.append(",Type=" + Type);
            sb.append(",NextStep=" + NextStep);
            sb.append(",LeadSource=" + LeadSource);
            sb.append(",IsWon=" + String.valueOf(IsWon));
            sb.append(",CampaignId=" + CampaignId);
            sb.append(",OwnerId=" + OwnerId);
            sb.append(",Territory2Id=" + Territory2Id);
            sb.append(",CreatedDate=" + String.valueOf(CreatedDate));
            sb.append(",FiscalQuarter=" + String.valueOf(FiscalQuarter));
            sb.append(",FiscalYear=" + String.valueOf(FiscalYear));
            sb.append(",Discount__c=" + String.valueOf(Discount__c));
            sb.append(",Competitor__c=" + Competitor__c);
            sb.append(",Age__c=" + String.valueOf(Age__c));
            sb.append("]");

*/
          case _ => throw new Exception(s"Unexpected event ${event} is read.")
        }
      } catch {
        case e: Exception => {
          logger.error(s"Cannot convert ${event} to Rating. Exception: ${e}.")
          throw e
        }
      }
      customer
    }.cache()

    new TrainingData(customersRDD)
  }
}

@SerialVersionUID(9129684718267757690L) case class Customer(
   id: Option[String],
   accountId: Option[String],
   name: Option[String],
   description: Option[String],
   amount: Option[Double],
   closeDate: Option[DateTimeFormat],
   opportunityType: Option[String],
   nextStep: Option[String],
   leadSource: Option[String],
   isWon: Option[Boolean],
   campaignId: Option[String],
   ownerId: Option[String],
   territory: Option[String],
   createdDate: Option[DateTimeFormat],
   fiscalQuarter: Option[String],
   fiscalYear: Option[String],
   discount: Option[Double],
   competitor: Option[String],
   age: Option[Double]) extends Serializable

class TrainingData(
                    val customers: RDD[Customer]
                    ) extends Serializable {
  override def toString = {
    s"customers: [${customers.count()}] (${customers.take(2).toList}...)"
  }
}