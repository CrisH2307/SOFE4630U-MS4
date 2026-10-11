# Milestone 4: Smart Meter Preprocessing Pipeline

- Link: https://github.com/CrisH2307/ENGR5520_M4.git

## 1. System Design and Architecture
The preprocessing pipeline was migrated from a monolithic Dataflow job to an event-driven microservices architecture hosted on Google Kubernetes Engine (GKE). To satisfy the constraint of utilizing a single Google Cloud Pub/Sub topic (`smart-meter-topic`), the architecture implements **Attribute-Based Message Routing**. 

Instead of separate queues, pipeline progression is controlled by custom message attributes:
*   **FilterReading Microservice:** Subscribes to messages with `status="raw"`. It validates the JSON payload and drops any records containing `null` or missing measurements. Valid records are re-published to the topic with `status="filtered"`.
*   **ConvertReading Microservice:** Subscribes to messages with `status="filtered"`. It applies mathematical unit conversions to the pressure and temperature fields, then re-publishes the payload with `status="processed"`.
*   **BigQuery Subscription:** A native Pub/Sub subscription listens for `status="processed"` and automatically inserts the final schema-mapped JSON directly into the BigQuery `processed_readings` table.

## 2. Pipeline Verification and Testing
To verify the routing and logic, test payloads were injected directly into the Pub/Sub topic using the Google Cloud CLI. 

**Step 1: Injecting a valid record (meter-456)**
```bash
$ gcloud pubsub topics publish smart-meter-topic \
  --message='{"meter_id": "meter-456", "pressure": 100.0, "temperature": 20.0}' \
  --attribute=status="raw"

messageIds:
- '21524475918485019'
```

**Step 2: Injecting an invalid record (meter-777) with a null pressure value**
```bash
$ gcloud pubsub topics publish smart-meter-topic \
  --message='{"meter_id": "meter-777", "pressure": null, "temperature": 20.0}' \
  --attribute=status="raw"

messageIds:
- '21524263973695406'
```

**Step 3: Verifying final BigQuery output**
The database query confirms that the `FilterReading` microservice successfully intercepted and dropped the invalid `meter-777` record. The `ConvertReading` microservice successfully processed `meter-456`, converting 100.0 kPa to 14.5 psi, and 20.0°C to 68.0°F.

```bash
$ bq query --use_legacy_sql=false \
'SELECT * FROM `project-4cfdac74-a7d9-4d41-abf.smart_meter_dataset.processed_readings`'

+-----------+----------+-------------+
| meter_id  | pressure | temperature |
+-----------+----------+-------------+
| meter-456 |     14.5 |        68.0 |
+-----------+----------+-------------+
```

## 3. Discussion: Dataflow vs. Microservices

### Apache Beam / Google Cloud Dataflow
**Advantages:**
*   **Advanced Streaming Mechanics:** Dataflow natively supports complex streaming paradigms, including time-based windowing, handling late-arriving data, and watermark tracking.
*   **Exactly-Once Processing:** It provides strict guarantees that data is processed exactly once, which is critical for financial or exact-count metrics.
*   **Managed Infrastructure:** As a serverless runner, it eliminates the need to manually provision, scale, or maintain underlying compute clusters.

**Disadvantages:**
*   **Steeper Learning Curve:** The Apache Beam SDK requires learning specialized programmatic paradigms for defining pipelines, transforms, and PCollections.
*   **Cost/Overhead for Simple Tasks:** Spinning up Dataflow workers for highly simplistic, stateless transformations (like a basic math conversion) can introduce unnecessary baseline latency and cost.

### Event-Driven Microservices (GKE + Pub/Sub)
**Advantages:**
*   **Flexibility and Portability:** Microservices are packaged in standard Docker containers, making them language-agnostic and highly portable across any Kubernetes cluster, mitigating vendor lock-in.
*   **Decoupled Debugging:** Each transformation step is an isolated container. If the conversion logic breaks, the `ConvertReading` pod can be rolled back without impacting the `FilterReading` pod's ability to continue consuming raw data.
*   **Efficiency for Stateless Operations:** For simple, stateless event triggers, lightweight containers provide near-instantaneous processing without the overhead of initializing a massive distributed processing job.

**Disadvantages:**
*   **Significant DevOps Overhead:** Requires manual configuration of Kubernetes deployments, networking, IAM service accounts, and Pub/Sub subscription routing.
*   **Complex Aggregations:** Implementing time-windowed aggregations (e.g., "calculate the average temperature over the last 5 minutes") is extremely difficult using independent stateless microservices compared to Dataflow's native windowing functions.