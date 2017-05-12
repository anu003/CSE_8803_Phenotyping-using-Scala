/**
  * @author Hang Su <hangsu@gatech.edu>,
  * @author Sungtae An <stan84@gatech.edu>,
  */

package edu.gatech.cse8803.phenotyping

import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object T2dmPhenotype {
  
  // criteria codes given
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43", "250.51", "250.53", "250.61",
      "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6", "250.62", "250.5", "250.52",
      "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl", "glucatrol ", "glyburide", "micronase",
      "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl", "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone",
      "acarbose", "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide", "avandia", "actos", "actos", "glipizide")

  val DM_RELATED = Set("790.21", "790.22", "790.2", "790.29", "648.81", "648.82", "648.83", "648.84", "648", "648", "648.01", "648.02", "648.03",
    "648.04", "791.5", "277.7", "V77.1", "256.4")

  /**
    * Transform given data set to a RDD of patients and corresponding phenotype
    * @param medication medication RDD
    * @param labResult lab result RDD
    * @param diagnostic diagnostic code RDD
    * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
    */

  def abnormal_lab(labResult1:RDD[LabResult]):RDD[String] ={
         val sc = labResult1.sparkContext
         val cal1 = labResult1.filter(r => r.testName == "HbA1c" && r.value >= 6.0).map(r => r.patientID).distinct()
         val cal2 = labResult1.filter(r => r.testName == "Hemoglobin A1c" && r.value >= 6.0).map(r => r.patientID).distinct()
         val cal3 = labResult1.filter(r => r.testName == "Fasting Glucose" && r.value >= 110).map(r => r.patientID).distinct()
         val cal4 = labResult1.filter(r => r.testName == "Fasting blood glucose" && r.value >= 110).map(r => r.patientID).distinct()
         val cal5 = labResult1.filter(r => r.testName == "fasting plasma glucose" && r.value >= 110).map(r => r.patientID).distinct()
         val cal6 = labResult1.filter(r => r.testName == "Glucose" && r.value > 110).map(r => r.patientID).distinct()
         val cal7 = labResult1.filter(r => r.testName == "Glucose, Serum" && r.value > 110).map(r => r.patientID).distinct()
         val cal = sc.union(cal1,cal2,cal3,cal4,cal5,cal6,cal7).distinct()
         cal
      }

  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
      * Remove the place holder and implement your code here.
      * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
      * When testing your code, we expect your function to have no side effect,
      * i.e. do NOT read from file or write file
      *
      * You don't need to follow the example placeholder code below exactly, but do have the same return type.
      *
      * Hint: Consider case sensitivity when doing string comparisons.
      */

    val sc = medication.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    /** Hard code the criteria */
    val type1_dm_dx = Set("code1", "250.03")
    val type1_dm_med = Set("med1", "insulin nph")

    val patients_all = medication.map(_.patientID).union(labResult.map(_.patientID)).union(diagnostic.map(_.patientID)).distinct()

    val medmap = medication.map(r => (r.patientID, r))
    val labmap = labResult.map(r => (r.patientID, r))
    val diagmap = diagnostic.map(r => (r.patientID, r))

    //Case part
    val T1DM_DX_YES = diagnostic.filter(r => T1DM_DX.contains(r.code)).map(_.patientID).distinct()
    val T1DM_DX_NO = patients_all.subtract(T1DM_DX_YES).distinct()

    val T2DM_DX_YES= diagnostic.filter(r => T2DM_DX.contains(r.code)).map(_.patientID).distinct()
    val T2DM_DX_NO = patients_all.subtract(T2DM_DX_YES).distinct()

    val T1DM_MED_YES = medication.filter(r => T1DM_MED.contains(r.medicine.toLowerCase)).map(_.patientID).distinct()
    val T1DM_MED_NO = patients_all.subtract(T1DM_MED_YES).distinct()

    val T2DM_MED_YES= medication.filter(r => T2DM_MED.contains(r.medicine.toLowerCase)).map(_.patientID).distinct()
    val T2DM_MED_NO = patients_all.subtract(T2DM_MED_YES).distinct()

    val part1 = T1DM_DX_NO.intersection(T2DM_DX_YES).intersection(T1DM_MED_NO)
    val part2= T1DM_DX_NO.intersection(T2DM_DX_YES).intersection(T1DM_MED_YES).intersection(T2DM_MED_NO)
    val part3_1= T1DM_DX_NO.intersection(T2DM_DX_YES).intersection(T1DM_MED_YES).intersection(T2DM_MED_YES)
    val sub_med = medmap.join(part3_1.map(r=>(r, 0))).map(r => Medication(r._2._1.patientID, r._2._1.date, r._2._1.medicine))
    val part3_1_1 = sub_med.filter(r => T1DM_MED.contains(r.medicine.toLowerCase)).map(r => (r.patientID, r.date.getTime())).reduceByKey(Math.min)
    val part3_1_2 = sub_med.filter(r => T2DM_MED.contains(r.medicine.toLowerCase)).map(r => (r.patientID, r.date.getTime())).reduceByKey(Math.min)
    val part3 = part3_1_1.join(part3_1_2).filter(r=> r._2._1 > r._2._2).map(_._1)

    val case_patients= sc.union(part1, part2, part3).distinct()

    //Control part
    val step1 = labResult.filter(r => r.testName.toLowerCase.contains("glucose")).map(_.patientID).distinct()

    val step2_1:RDD[LabResult] = labmap.join(patients_all.map(r => (r, 0))).map(r => LabResult(r._2._1.patientID, r._2._1.date, r._2._1.testName, r._2._1.value)).distinct()
    val step2_2 = abnormal_lab(step2_1)
    val step2 = step1.subtract(step2_2).distinct()

    val step3_diags = diagmap.join(patients_all.map(r => (r, 0))).map(r => Diagnostic(r._2._1.patientID, r._2._1.date, r._2._1.code))
    val step3_1 = step3_diags.filter(r => DM_RELATED.contains(r.code)).map(r => r.patientID).distinct()
    val step3_2 = diagnostic.filter(r => r.code.startsWith("250")).map(r => r.patientID).distinct()
    val step3 = patients_all.subtract(step3_1.union(step3_2)).distinct()

    val control_patients = step2.intersection(step3)

    //Other part
    val others1 = patients_all.subtract(case_patients).subtract(control_patients).distinct()

    /** Find CASE Patients */
    val casePatients = case_patients.map((_,1))

    /** Find CONTROL Patients */
    val controlPatients = control_patients.map((_,2))

    /** Find OTHER Patients */
    val others = others1.map((_,3))

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(casePatients, controlPatients, others)

    /** Return */
    phenotypeLabel
  }
}