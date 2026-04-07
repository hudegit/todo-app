# OpenShift Deployment Guide for Todo App with Kafka

## Prerequisites
1. OpenShift CLI (`oc`) installed and logged in
2. Access to an OpenShift project/namespace
3. PostgreSQL and Kafka instances available in your cluster

## Deployment Steps

### 1. Create a new OpenShift project
```bash
oc new-project todo-app
```

### 2. Set up PostgreSQL
You can either:
- Use an existing PostgreSQL service in your cluster
- Deploy a new PostgreSQL instance using the OpenShift template:

```bash
oc new-app --template=postgresql-persistent \
  -p POSTGRESQL_USER=postgres \
  -p POSTGRESQL_PASSWORD=postgres \
  -p POSTGRESQL_DATABASE=todo_db \
  -p VOLUME_CAPACITY=1Gi
```

### 3. Set up Kafka
If you don't have Kafka in your cluster, you can deploy Strimzi:

```bash
# Install Strimzi operator
oc apply -f 'https://strimzi.io/install/latest?namespace=todo-app' -n todo-app

# Deploy Kafka cluster
oc apply -f - <<EOF
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: todo-kafka
spec:
  kafka:
    version: 3.4.0
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      default.replication.factor: 3
      min.insync.replicas: 2
      inter.broker.protocol.version: "3.4"
    storage:
      type: jbod
      volumes:
      - id: 0
        type: persistent-claim
        size: 10Gi
        deleteClaim: false
  zookeeper:
    replicas: 3
    storage:
      type: persistent-claim
      size: 10Gi
      deleteClaim: false
  entityOperator:
    topicOperator: {}
    userOperator: {}
EOF
```

### 4. Create a ConfigMap for environment variables
```bash
oc create configmap todo-app-config \
  --from-literal=DB_HOST=postgresql.todo-app.svc.cluster.local \
  --from-literal=DB_NAME=todo_db \
  --from-literal=DB_USER=postgres \
  --from-literal=DB_PASSWORD=postgres \
  --from-literal=DB_PORT=5432 \
  --from-literal=KAFKA_BOOTSTRAP_SERVERS=todo-kafka-kafka-bootstrap.todo-app.svc.cluster.local:9092
```

### 5. Build and deploy the Flask app
```bash
# Create a new app from your source code
oc new-app python:3.9~https://github.com/your-repo/todo-app.git \
  --name=todo-app \
  --env-from=configmap/todo-app-config

# Expose the service
oc expose svc/todo-app
```

### 6. Deploy the Kafka consumer
```bash
# Create a deployment for the consumer
oc new-app python:3.9~https://github.com/your-repo/todo-app.git \
  --name=todo-consumer \
  --command="python consumer.py" \
  --env-from=configmap/todo-app-config
```

### 7. Verify the deployment
```bash
# Check pods
oc get pods

# Check logs for the Flask app
oc logs -f dc/todo-app

# Check logs for the consumer
oc logs -f dc/todo-consumer
```

### 8. Access the application
```bash
# Get the route URL
oc get route todo-app
```

## Notes
1. Make sure to replace the GitHub repository URL with your actual repository.
2. Adjust resource requests/limits as needed for your environment.
3. For production, consider adding:
   - Proper secrets management (instead of ConfigMap)
   - Health checks and readiness probes
   - Horizontal pod autoscaling
   - Monitoring and logging