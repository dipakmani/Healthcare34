Healthcare & Pharma Data Generator

4 Fact tables, each linked to 15+ Dimension tables

Generates 500,000 rows per fact by default and writes CSVs

Author: ChatGPT

import pandas as pd import numpy as np import random from datetime import datetime, timedelta

random.seed(42) np.random.seed(42)

=============================

CONFIG

=============================

FACT_ROWS = 500_000  # rows per fact

Dimension sizes (tune for your machine if needed)

N_PATIENTS = 100_000 N_DOCTORS = 10_000 N_HOSPITALS = 1_000 N_DEPARTMENTS = 120 N_DIAGNOSES = 5_000 N_INSURERS = 500 N_ROOMS = 3_000 N_NURSES = 5_000 N_TREAT_PLANS = 2_000 N_BILLING_CODES = 1_500 N_REFERRALS = 2_000 N_EMER_CONTACTS = 120_000 N_LOCATIONS = 1_000 N_PAYMENT_MODES = 8 N_PROCEDURES = 2_000 N_EQUIPMENT = 2_500 N_ANESTHESIA = 8 N_OPERATION_ROOMS = 600 N_SURG_TEAMS = 1_200 N_POSTOP = 20 N_PHARMACIES = 2_000 N_DRUGS = 10_000 N_DOSAGE = 50 N_MANUFACTURERS = 400 N_RX_TYPES = 6 N_ALLERGIES = 40 N_LABS = 500 N_TEST_TYPES = 200 N_SPECIMEN = 25 N_TECHNICIANS = 3_000 N_RESULT_CAT = 12

Date dimension span (days)

DATE_START = datetime(2022, 1, 1) DATE_DAYS = 1_400  # ~3.8 years

OUT_DIR = "."  # change to your folder if needed

=============================

HELPERS

=============================

def make_ids(prefix, n): return np.array([f"{prefix}{i:06d}" for i in range(1, n + 1)], dtype=object)

def make_date_dim(start: datetime, days: int) -> pd.DataFrame: dates = [start + timedelta(days=i) for i in range(days)] df = pd.DataFrame({ "DateID": [int(d.strftime("%Y%m%d")) for d in dates], "FullDate": dates, }) df["Year"] = df["FullDate"].dt.year df["Month"] = df["FullDate"].dt.month df["Day"] = df["FullDate"].dt.day df["Quarter"] = ((df["Month"] - 1) // 3) + 1 df["Weekday"] = df["FullDate"].dt.day_name() df["IsWeekend"] = df["Weekday"].isin(["Saturday", "Sunday"]).astype(int) return df

def choice(a, size): """Vectorized random choice over numpy arrays/list-like.""" idx = np.random.randint(0, len(a), size=size) return np.asarray(a)[idx]

=============================

DIMENSIONS (UNION FOR ALL FACTS)

=============================

print("Creating dimensions...")

DimDate = make_date_dim(DATE_START, DATE_DAYS)

DimPatient = pd.DataFrame({ "PatientID": make_ids("PAT", N_PATIENTS), "FullName": [f"Patient_{i}" for i in range(1, N_PATIENTS + 1)], "Gender": choice(["Male", "Female", "Other"], N_PATIENTS), "Age": np.random.randint(1, 96, size=N_PATIENTS), "BloodGroup": choice(["A+","A-","B+","B-","O+","O-","AB+","AB-"], N_PATIENTS), })

DimDoctor = pd.DataFrame({ "DoctorID": make_ids("DOC", N_DOCTORS), "FullName": [f"Doctor_{i}" for i in range(1, N_DOCTORS + 1)], "Specialization": choice(["Cardiology","Neurology","Orthopedics","Pediatrics","Oncology","General"], N_DOCTORS), "YearsExperience": np.random.randint(1, 41, size=N_DOCTORS), })

DimHospital = pd.DataFrame({ "HospitalID": make_ids("HOS", N_HOSPITALS), "HospitalName": [f"Hospital_{i}" for i in range(1, N_HOSPITALS + 1)], "Type": choice(["Private","Government","Trust"], N_HOSPITALS), "BedCapacity": np.random.randint(50, 1501, size=N_HOSPITALS), })

DimDepartment = pd.DataFrame({ "DepartmentID": make_ids("DEP", N_DEPARTMENTS), "DepartmentName": choice(["Emergency","Surgery","General Medicine","ICU","OPD","Cardiology","Neurology","Oncology","Ortho"], N_DEPARTMENTS), "FloorNumber": np.random.randint(1, 15, size=N_DEPARTMENTS), })

DimDiagnosis = pd.DataFrame({ "DiagnosisID": make_ids("DIA", N_DIAGNOSES), "DiagnosisCode": [f"D{1000+i}" for i in range(N_DIAGNOSES)], "Category": choice(["Acute","Chronic","Injury","Infection"], N_DIAGNOSES), })

DimInsurance = pd.DataFrame({ "InsuranceProviderID": make_ids("INS", N_INSURERS), "ProviderName": [f"Insurer_{i}" for i in range(1, N_INSURERS + 1)], "PlanType": choice(["Gold","Silver","Platinum","Basic"], N_INSURERS), "CoveragePercent": np.random.randint(40, 101, size=N_INSURERS), })

DimRoom = pd.DataFrame({ "RoomID": make_ids("ROOM", N_ROOMS), "RoomType": choice(["General","Semi-Private","Private","ICU"], N_ROOMS), "Floor": np.random.randint(1, 15, size=N_ROOMS), })

DimNurse = pd.DataFrame({ "NurseID": make_ids("NUR", N_NURSES), "Grade": choice(["RN","LPN","CNS","NP"], N_NURSES), "YearsExperience": np.random.randint(1, 31, size=N_NURSES), })

DimTreatmentPlan = pd.DataFrame({ "TreatmentPlanID": make_ids("TPL", N_TREAT_PLANS), "PlanType": choice(["Conservative","Surgical","Rehab","Mixed"], N_TREAT_PLANS), })

DimBillingCode = pd.DataFrame({ "BillingCodeID": make_ids("BIL", N_BILLING_CODES), "ServiceType": choice(["Consultation","Lab","Procedure","Pharmacy"], N_BILLING_CODES), "StandardRate": np.round(np.random.uniform(50, 5000, size=N_BILLING_CODES), 2), })

DimReferral = pd.DataFrame({ "ReferralID": make_ids("REF", N_REFERRALS), "ReferralSource": choice(["Internal","External","Self"], N_REFERRALS), })

DimEmergencyContact = pd.DataFrame({ "EmergencyContactID": make_ids("EC", N_EMER_CONTACTS), "Relation": choice(["Parent","Spouse","Sibling","Friend","Guardian"], N_EMER_CONTACTS), })

DimLocation = pd.DataFrame({ "LocationID": make_ids("LOC", N_LOCATIONS), "Region": choice(["North","South","East","West","Central"], N_LOCATIONS), })

DimPaymentMode = pd.DataFrame({ "PaymentModeID": make_ids("PAY", N_PAYMENT_MODES), "Method": ["Cash","Card","UPI","NetBanking","Insurance","Wallet","Cheque","Other"], })

DimProcedure = pd.DataFrame({ "ProcedureID": make_ids("PRC", N_PROCEDURES), "ProcedureType": choice(["Surgical","Diagnostic","Therapeutic"], N_PROCEDURES), "StandardCost": np.round(np.random.uniform(1000, 50000, size=N_PROCEDURES), 2), })

DimEquipment = pd.DataFrame({ "EquipmentID": make_ids("EQP", N_EQUIPMENT), "EquipmentType": choice(["MRI","CT","XRay","Ultrasound","Ventilator","Monitor"], N_EQUIPMENT), })

DimAnesthesia = pd.DataFrame({ "AnesthesiaID": make_ids("ANE", N_ANESTHESIA), "AnesthesiaType": ["General","Local","Regional","Sedation","Spinal","Epidural","TIVA","None"], })

DimOperationRoom = pd.DataFrame({ "OperationRoomID": make_ids("OR", N_OPERATION_ROOMS), "ORType": choice(["Minor","Major","Hybrid","Endoscopy"], N_OPERATION_ROOMS), })

DimSurgicalTeam = pd.DataFrame({ "SurgicalTeamID": make_ids("ST", N_SURG_TEAMS), "TeamLevel": choice(["A","B","C","D"], N_SURG_TEAMS), })

DimPostOpCare = pd.DataFrame({ "PostOpCareID": make_ids("POC", N_POSTOP), "CareLevel": ["ICU","HDU","Ward","DayCare","Home","Telehealth","Rehab","StepDown","Isolation","Observation", "ICU+Vent","ICU+Dialysis","HDU+Monitor","Ward+Physio","Ward+Diet","Home+Nurse","Home+Device","Rehab+OT","Rehab+PT","Isolation+PPE"], })

DimPharmacy = pd.DataFrame({ "PharmacyID": make_ids("PHM", N_PHARMACIES), "Type": choice(["In-Hospital","Retail"], N_PHARMACIES), })

DimDrug = pd.DataFrame({ "DrugID": make_ids("DRG", N_DRUGS), "Category": choice(["Antibiotic","Analgesic","Antidepressant","Antihypertensive","Antidiabetic"], N_DRUGS), })

DimDosage = pd.DataFrame({ "DosageID": make_ids("DOS", N_DOSAGE), "DosageForm": ["Tablet","Capsule","Syrup","Injection","IV","Topical","Inhaler","Drops","Patch","Suppository", "Ointment","Cream","Gel","Lotion","Spray","Powder","Granules","Suspension","Solution","Elixir", "Lozenge","Toothpaste","Mouthwash","Gargle","Shampoo","Foam","Paste","Emulsion","Aerosol","Bar", "Pasteule","Implant","Chewable","Effervescent","ExtendedRelease","ImmediateRelease","Sublingual","Buccal","Nasal","Rectal", "Vaginal","Otic","Ophthalmic","Transdermal","Subcutaneous","Intramuscular","Intravenous","Intradermal","Intrathecal","Other"], })

DimManufacturer = pd.DataFrame({ "ManufacturerID": make_ids("MF", N_MANUFACTURERS), "ManufacturerName": [f"MFG_{i}" for i in range(1, N_MANUFACTURERS + 1)], })

DimPrescriptionType = pd.DataFrame({ "PrescriptionTypeID": make_ids("PT", N_RX_TYPES), "Type": ["Acute","Chronic","OTC","PRN","Stat","Taper"], })

DimAllergy = pd.DataFrame({ "AllergyID": make_ids("ALG", N_ALLERGIES), "Allergen": [ "Penicillin","Sulfa","NSAIDs","Aspirin","Latex","Peanuts","Shellfish","Milk","Eggs","Soy", "Gluten","Pollen","Dust","Mold","BeeSting","Nickel","Perfume","Cats","Dogs","Wool", "Acrylic","Cobalt","Formaldehyde","Gold","Lanolin","Parabens","Thimerosal","Neomycin","Fragrance","Dyes", "Strawberry","Banana","Kiwi","Avocado","Chocolate","Citrus","Corn","Sunlight","Cold","Heat" ], })

DimLab = pd.DataFrame({ "LabID": make_ids("LAB", N_LABS), "LabType": choice(["Pathology","Radiology","Microbiology","Biochemistry","Hematology"], N_LABS), })

DimTestType = pd.DataFrame({ "TestTypeID": make_ids("TST", N_TEST_TYPES), "Name": [f"TestType_{i}" for i in range(1, N_TEST_TYPES + 1)], })

DimSpecimen = pd.DataFrame({ "SpecimenID": make_ids("SPC", N_SPECIMEN), "Specimen": ["Blood","Urine","Stool","Sputum","CSF","Serum","Plasma","Tissue","BoneMarrow","Saliva", "Pleural","Peritoneal","Synovial","Semen","Vaginal","Nasopharyngeal","Throat","Hair","Nail","Skin", "Sweat","Tear","Bile","Gastric","Other"], })

DimTechnician = pd.DataFrame({ "TechnicianID": make_ids("TECH", N_TECHNICIANS), "Grade": choice(["T1","T2","T3","T4"], N_TECHNICIANS), })

DimResultCategory = pd.DataFrame({ "ResultCategoryID": make_ids("RES", N_RESULT_CAT), "Category": ["Normal","Abnormal","Critical","Positive","Negative","Borderline","Inconclusive","Repeat","Contaminated","Hemolyzed","Clotted","Other"], })

Save dimensions

print("Saving dimensions...") for name, df in { "DimDate": DimDate, "DimPatient": DimPatient, "DimDoctor": DimDoctor, "DimHospital": DimHospital, "DimDepartment": DimDepartment, "DimDiagnosis": DimDiagnosis, "DimInsurance": DimInsurance, "DimRoom": DimRoom, "DimNurse": DimNurse, "DimTreatmentPlan": DimTreatmentPlan, "DimBillingCode": DimBillingCode, "DimReferral": DimReferral, "DimEmergencyContact": DimEmergencyContact, "DimLocation": DimLocation, "DimPaymentMode": DimPaymentMode, "DimProcedure": DimProcedure, "DimEquipment": DimEquipment, "DimAnesthesia": DimAnesthesia, "DimOperationRoom": DimOperationRoom, "DimSurgicalTeam": DimSurgicalTeam, "DimPostOpCare": DimPostOpCare, "DimPharmacy": DimPharmacy, "DimDrug": DimDrug, "DimDosage": DimDosage, "DimManufacturer": DimManufacturer, "DimPrescriptionType": DimPrescriptionType, "DimAllergy": DimAllergy, "DimLab": DimLab, "DimTestType": DimTestType, "DimSpecimen": DimSpecimen, "DimTechnician": DimTechnician, "DimResultCategory": DimResultCategory, }.items(): df.to_csv(f"{OUT_DIR}/{name}.csv", index=False)

=============================

FACTS (EACH WITH 15+ DIMENSIONS)

=============================

--- helper to pick two dates with discharge >= admission

all_dates = DimDate["DateID"].to_numpy()

def pick_adm_dis(size): idx_adm = np.random.randint(0, len(all_dates) - 10, size=size) los = np.random.randint(0, 15, size=size)  # 0-14 days idx_dis = idx_adm + los idx_dis = np.clip(idx_dis, 0, len(all_dates) - 1) return all_dates[idx_adm], all_dates[idx_dis], los

print("Generating Fact_Visits...")

Fact_Visits with 15 dims

adm, dis, los = pick_adm_dis(FACT_ROWS) Fact_Visits = pd.DataFrame({ "VisitID": np.arange(1, FACT_ROWS + 1), "AdmissionDateID": adm, "DischargeDateID": dis, # 15+ dimension FKs "PatientID": choice(DimPatient["PatientID"], FACT_ROWS), "DoctorID": choice(DimDoctor["DoctorID"], FACT_ROWS), "HospitalID": choice(DimHospital["HospitalID"], FACT_ROWS), "DepartmentID": choice(DimDepartment["DepartmentID"], FACT_ROWS), "DiagnosisID": choice(DimDiagnosis["DiagnosisID"], FACT_ROWS), "InsuranceProviderID": choice(DimInsurance["InsuranceProviderID"], FACT_ROWS), "VisitDateID": choice(DimDate["DateID"], FACT_ROWS), "RoomID": choice(DimRoom["RoomID"], FACT_ROWS), "NurseID": choice(DimNurse["NurseID"], FACT_ROWS), "TreatmentPlanID": choice(DimTreatmentPlan["TreatmentPlanID"], FACT_ROWS), "BillingCodeID": choice(DimBillingCode["BillingCodeID"], FACT_ROWS), "ReferralID": choice(DimReferral["ReferralID"], FACT_ROWS), "EmergencyContactID": choice(DimEmergencyContact["EmergencyContactID"], FACT_ROWS), "LocationID": choice(DimLocation["LocationID"], FACT_ROWS), "PaymentModeID": choice(DimPaymentMode["PaymentModeID"], FACT_ROWS), # measures "LengthOfStay_Days": los, "WaitTime_Min": np.random.randint(0, 301, size=FACT_ROWS), "VisitCost": np.round(np.random.uniform(100, 10_000, size=FACT_ROWS), 2), "SatisfactionScore": np.random.randint(1, 11, size=FACT_ROWS), "EmergencyFlag": np.random.randint(0, 2, size=FACT_ROWS), }) Fact_Visits.to_csv(f"{OUT_DIR}/Fact_Visits.csv", index=False)

print("Generating Fact_Operations...")

Fact_Operations with 15 dims

op_date = choice(DimDate["DateID"], FACT_ROWS) Fact_Operations = pd.DataFrame({ "OperationID": np.arange(1, FACT_ROWS + 1), # 15+ dimension FKs "VisitID": choice(Fact_Visits["VisitID"], FACT_ROWS), "PatientID": choice(DimPatient["PatientID"], FACT_ROWS), "DoctorID": choice(DimDoctor["DoctorID"], FACT_ROWS), "HospitalID": choice(DimHospital["HospitalID"], FACT_ROWS), "DepartmentID": choice(DimDepartment["DepartmentID"], FACT_ROWS), "DiagnosisID": choice(DimDiagnosis["DiagnosisID"], FACT_ROWS), "InsuranceProviderID": choice(DimInsurance["InsuranceProviderID"], FACT_ROWS), "ProcedureID": choice(DimProcedure["ProcedureID"], FACT_ROWS), "EquipmentID": choice(DimEquipment["EquipmentID"], FACT_ROWS), "AnesthesiaID": choice(DimAnesthesia["AnesthesiaID"], FACT_ROWS), "OperationRoomID": choice(DimOperationRoom["OperationRoomID"], FACT_ROWS), "SurgicalTeamID": choice(DimSurgicalTeam["SurgicalTeamID"], FACT_ROWS), "PostOpCareID": choice(DimPostOpCare["PostOpCareID"], FACT_ROWS), "LocationID": choice(DimLocation["LocationID"], FACT_ROWS), "DateID": op_date, # measures "OperationDuration_Min": np.random.randint(15, 720, size=FACT_ROWS), "OperationCost": np.round(np.random.uniform(1_000, 200_000, size=FACT_ROWS), 2), "AnesthesiaMinutes": np.random.randint(10, 600, size=FACT_ROWS), "BloodLoss_ml": np.random.randint(0, 2_000, size=FACT_ROWS), "OutcomeScore": np.random.randint(1, 6, size=FACT_ROWS), "ComplicationFlag": np.random.randint(0, 2, size=FACT_ROWS), }) Fact_Operations.to_csv(f"{OUT_DIR}/Fact_Operations.csv", index=False)

print("Generating Fact_Medications...")

Fact_Medications with 15 dims

rx_date = choice(DimDate["DateID"], FACT_ROWS) Fact_Medications = pd.DataFrame({ "MedicationFactID": np.arange(1, FACT_ROWS + 1), # 15+ dimension FKs "VisitID": choice(Fact_Visits["VisitID"], FACT_ROWS), "PatientID": choice(DimPatient["PatientID"], FACT_ROWS), "DoctorID": choice(DimDoctor["DoctorID"], FACT_ROWS), "HospitalID": choice(DimHospital["HospitalID"], FACT_ROWS), "DepartmentID": choice(DimDepartment["DepartmentID"], FACT_ROWS), "DiagnosisID": choice(DimDiagnosis["DiagnosisID"], FACT_ROWS), "InsuranceProviderID": choice(DimInsurance["InsuranceProviderID"], FACT_ROWS), "PharmacyID": choice(DimPharmacy["PharmacyID"], FACT_ROWS), "DrugID": choice(DimDrug["DrugID"], FACT_ROWS), "DosageID": choice(DimDosage["DosageID"], FACT_ROWS), "ManufacturerID": choice(DimManufacturer["ManufacturerID"], FACT_ROWS), "PrescriptionTypeID": choice(DimPrescriptionType["PrescriptionTypeID"], FACT_ROWS), "AllergyID": choice(DimAllergy["AllergyID"], FACT_ROWS), "LocationID": choice(DimLocation["LocationID"], FACT_ROWS), "DateID": rx_date, # measures "Quantity": np.random.randint(1, 91, size=FACT_ROWS), "DaysSupply": np.random.randint(1, 61, size=FACT_ROWS), "UnitPrice": np.round(np.random.uniform(1, 200, size=FACT_ROWS), 2), "DrugCost": lambda: 0,  # to be computed "InsuranceCovered": np.round(np.random.uniform(0, 150, size=FACT_ROWS), 2), "PatientPayable": np.round(np.random.uniform(0, 150, size=FACT_ROWS), 2), "AdherenceScore": np.random.randint(0, 101, size=FACT_ROWS), })

compute DrugCost = Quantity * UnitPrice

Fact_Medications["DrugCost"] = np.round(Fact_Medications["Quantity"] * Fact_Medications["UnitPrice"], 2) Fact_Medications.to_csv(f"{OUT_DIR}/Fact_Medications.csv", index=False)

print("Generating Fact_LabTests...")

Fact_LabTests with 15 dims

lab_date = choice(DimDate["DateID"], FACT_ROWS) Fact_LabTests = pd.DataFrame({ "LabTestID": np.arange(1, FACT_ROWS + 1), # 15+ dimension FKs "VisitID": choice(Fact_Visits["VisitID"], FACT_ROWS), "PatientID": choice(DimPatient["PatientID"], FACT_ROWS), "DoctorID": choice(DimDoctor["DoctorID"], FACT_ROWS), "HospitalID": choice(DimHospital["HospitalID"], FACT_ROWS), "DepartmentID": choice(DimDepartment["DepartmentID"], FACT_ROWS), "DiagnosisID": choice(DimDiagnosis["DiagnosisID"], FACT_ROWS), "InsuranceProviderID": choice(DimInsurance["InsuranceProviderID"], FACT_ROWS), "LabID": choice(DimLab["LabID"], FACT_ROWS), "TestTypeID": choice(DimTestType["TestTypeID"], FACT_ROWS), "SpecimenID": choice(DimSpecimen["SpecimenID"], FACT_ROWS), "TechnicianID": choice(DimTechnician["TechnicianID"], FACT_ROWS), "EquipmentID": choice(DimEquipment["EquipmentID"], FACT_ROWS), "ResultCategoryID": choice(DimResultCategory["ResultCategoryID"], FACT_ROWS), "LocationID": choice(DimLocation["LocationID"], FACT_ROWS), "DateID": lab_date, # measures "ResultValue": np.round(np.random.uniform(0, 100, size=FACT_ROWS), 2), "ReferenceLow": 10, "ReferenceHigh": 90, "Turnaround_Hours": np.random.randint(1, 168, size=FACT_ROWS), "AbnormalFlag": 0, })

Abnormal if value outside [10,90]

Fact_LabTests["AbnormalFlag"] = ((Fact_LabTests["ResultValue"] < Fact_LabTests["ReferenceLow"]) | (Fact_LabTests["ResultValue"] > Fact_LabTests["ReferenceHigh"]).astype(int)) Fact_LabTests.to_csv(f"{OUT_DIR}/Fact_LabTests.csv", index=False)

print("All CSVs written successfully.")

