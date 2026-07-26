# Security policy

This repository is a reference implementation and has no hosted service.
Please report security concerns privately through GitHub's security advisory
feature rather than a public issue.

Never use the local Compose credentials or `k8s/secret.example.yaml` in a real
environment. Supply database credentials through an external secret manager,
enable Kafka authentication and encryption, and complete a deployment-specific
threat model before any production evaluation.
