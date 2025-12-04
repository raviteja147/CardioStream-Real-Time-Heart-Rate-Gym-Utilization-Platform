# Real-Time Gym Workout Analytics – Kafka + Spark Streaming + Databricks Lakehouse

This project builds an end-to-end **real-time workout analytics platform** using **Kafka**, **Spark Structured Streaming**, and a **Databricks Lakehouse** (medallion architecture).

The platform ingests:
- Registration & login activity  
- User profile change data capture (CDC)  
- Continuous heart-rate (BPM) streams from wearables  
- Workout session events  

and produces curated analytical datasets such as **Workout BPM Summary** and **Gym Summary** for downstream analytics.

---

## 📌 Problem Statement & Requirements

![Problem statement and source events](images/problem-statement.png)

The goal is to:

1. **Design and implement a Lakehouse platform** using a **medallion architecture pattern** (Bronze → Silver → Gold).
2. **Ingest data from multiple source systems** (registration DB, wearables, mobile app, login/logout events) into the platform.
3. **Prepare analytics-ready datasets** for data consumers:
   - **Workout BPM Summary** – per-user heart rate statistics per workout.
   - **Gym Summary** – per-gym usage metrics and exercise time.

---

## 🏗️ High-Level Architecture

![Lakehouse architecture on ADLS + Databricks](images/lakehouse-architecture.png)

The Lakehouse is implemented on **Azure Databricks + ADLS Gen2** and logically structured as:

- `/sbit-metastore-root` – metastore root used by Databricks.
- `/sbit-managed-dev`
  - `/bronze_db` – raw ingested data (append-only).
  - `/silver_db` – cleaned and modeled data.
  - `/gold_db` – curated analytics tables (BPM & Gym summaries).
- `/sbit-unmanaged-dev`
  - `/data_zone` – landing zone / external data.
  - `/checkpoint_zone` – streaming checkpoints for reliability.

---

## 🔄 End-to-End Data Flow

![End-to-end data flow and tables](images/data-model.png)

1. **Registration (1)**  
   - Users register via an app; events are stored in **Azure SQL**.
   - Data Factory / ingestion jobs move this data into **Bronze** tables (`Users`, `Gym Logs`).

2. **User Profile CDC (2)**  
   - Profile updates are captured via **CDC** and streamed into a **Multiplex table**.
   - From there, Spark jobs normalize records into a **Silver `User Profile`** table.

3. **BPM Stream (3)**  
   - Wearables continuously send **BPM (beats per minute)** readings into **Kafka**.
   - **Kafka Connect** publishes these events to the `/kfk-mx` topic / multiplex table.
   - **Spark Structured Streaming** reads the BPM stream, enriches it with user and session context, and writes to the **`Workout BPM`** table.

4. **Workout Session Events (5)**  
   - Workout start/stop/session metadata is ingested into **`Workouts`** and **`Completed Workouts`** tables.

5. **Login / Logout (4 & 5)**  
   - Login and logout events are captured into **`Gym Logs`** to compute time spent at the gym.

6. **Gold Layer Aggregations**  
   - Using the Silver tables (`Users`, `User Profile`, `Heart Rate`, `Workouts`, `Workout BPM`, `Gym Logs`, `Completed Workouts`), Spark jobs build:
     - **`Workout BPM Summary`** (per-user, per-workout heart-rate statistics).
     - **`Gym Summary`** (per-gym daily usage & exercise duration).

---

## 📊 Analytical Outputs

### 1. Workout BPM Summary (Gold)

![Workout BPM Summary sample](images/workout-bpm-summary.png)

The **Workout BPM Summary** table aggregates heart-rate readings at the workout level.  
Typical columns include:

- `user_id`
- `date`
- `workout_id`
- `session_id`
- Demographics – `age_band`, `sex`, `city`, `state`
- `num_records` (#Rec) – number of BPM readings during the workout  
- `min_bpm`, `avg_bpm`, `max_bpm`

This dataset supports use cases such as:
- Monitoring workout intensity over time  
- Comparing heart-rate patterns by age group, gender, or location  
- Detecting potentially abnormal heart-rate behavior during workouts  

---

### 2. Gym Summary (Gold)

![Gym Summary sample](images/gym-summary.png)

The **Gym Summary** table aggregates per-gym usage metrics per day.  
Typical columns include:

- `gym_id`
- `mac_address` (gym device / beacon identifier)
- `date`
- `workouts` – list of workout IDs performed that day
- `minutes_in_gym` – total time users spent inside the gym
- `minutes_exercising` – total active workout minutes

This dataset can be used to:
- Analyze **gym utilization** across locations and days  
- Understand **active vs idle time** in the facility  
- Support capacity planning and equipment optimization  

---

## ⚙️ Technology Stack

- **Streaming & Messaging**
  - Apache Kafka
  - Kafka Connect

- **Processing**
  - Apache Spark / Spark Structured Streaming
  - Azure Databricks notebooks & jobs

- **Storage / Lakehouse**
  - Azure Data Lake Storage Gen2 (ADLS)
  - Medallion architecture: Bronze / Silver / Gold databases

- **Source Systems**
  - Azure SQL (registration, login/logout, some profile data)
  - Wearable devices and mobile app (BPM + workout sessions)

---

## 🧠 Key Concepts Demonstrated

- Building a **Lakehouse** with a **Medallion architecture** for streaming and batch data.
- Designing a **multi-source ingestion pipeline** (DB + CDC + Kafka).
- Using **Spark Structured Streaming** with **Kafka** and reliable checkpoints.
- Normalizing raw events into a clean **Silver data model** (users, workouts, heart rate, gym logs).
- Creating **Gold fact tables** optimized for BI & analytics (BPM & Gym summaries).

---

## 🚀 Possible Extensions

- Add **dashboards** (Power BI / Tableau) on top of the Gold tables.
- Implement **data quality checks** (e.g., minimum session length, BPM thresholds).
- Expose **REST APIs** over summaries for integration with external apps.
- Introduce **Delta Lake / Iceberg** for time-travel, schema evolution, and better ACID guarantees.

---

> 💡 _Note:_ Image paths in this README assume the images live under an `images/` folder.  
> If you place them elsewhere or use different names, update the `![...](images/...)` links accordingly.
